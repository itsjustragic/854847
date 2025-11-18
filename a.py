# app_with_postgres_fetch100.py
import os
import re
import time
import json
import secrets
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse

from jose import jwt
from passlib.context import CryptContext

from fastapi import FastAPI, Request, Depends, HTTPException, Query, status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from pydantic import BaseModel

# -------------------- Config --------------------
DATA_DIR = os.getenv("DATA_DIR", "data")
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

DATABASE_URL = os.getenv("DATABASE_URL")  # e.g. set by Render: postgres://user:pass@host:port/dbname
USE_DB = bool(DATABASE_URL)

SECRET_KEY = os.getenv("SECRET_KEY", secrets.token_urlsafe(32))
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# JSON file fallbacks (used when DATABASE_URL not set)
USERS_FILE = os.path.join(DATA_DIR, "users.json")
INVITES_FILE = os.path.join(DATA_DIR, "invites.json")
POSTS_FILE = os.path.join(DATA_DIR, "posts.json")
SAVED_URLS_FILE = os.path.join(DATA_DIR, "saved_urls.json")
SAVED_USER_URLS_FILE = os.path.join(DATA_DIR, "saved_user_urls.json")

# Headers & defaults
WEB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SnapStoryDL/1.0)",
    "Accept-Encoding": "identity"
}
DEFAULT_AVATAR = (
    "https://media.discordapp.net/attachments/1343576085098664020/"
    "1366204471633510530/IMG_20250427_190832_902.jpg?..."
)

# TikWM helper state (keeps latest caption)
LATEST_TIKWM_CAPTION: Dict[str, str] = {}
HD_URLS: Dict[str, str] = {}

# Logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# FastAPI
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# -------------------- DB helpers --------------------
def _get_db_conn():
    if not USE_DB:
        return None
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    return conn

def _init_db():
    if not USE_DB:
        logging.info("DATABASE_URL not set — using JSON-file fallback storage.")
        return
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password TEXT,
        avatar TEXT,
        fetched_at TIMESTAMP WITH TIME ZONE
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS invites (
        code TEXT PRIMARY KEY,
        used BOOLEAN NOT NULL DEFAULT FALSE
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS saved_urls (
        aweme_id TEXT PRIMARY KEY,
        play_url TEXT,
        hd_url TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS saved_user_urls (
        id SERIAL PRIMARY KEY,
        username TEXT,
        aweme_id TEXT,
        play_url TEXT,
        hd_url TEXT,
        images JSONB
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        username TEXT,
        aweme_id TEXT,
        data JSONB,
        PRIMARY KEY (username, aweme_id)
    );
    """)
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Database initialized (tables ensured)")

# -------------------- JSON fallback helpers --------------------
def load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# -------------------- Persistence API (DB-first, fallback to files) --------------------
# --- USERS ---
def load_users() -> Dict[str, dict]:
    if USE_DB:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT username, password, avatar, fetched_at FROM users;")
        rows = cur.fetchall()
        cur.close(); conn.close()
        return {r["username"]: {"password": r["password"], "avatar": r["avatar"], "fetched_at": r["fetched_at"].isoformat() if r["fetched_at"] else ""} for r in rows}
    return load_json(USERS_FILE, {})

def save_users(users: Dict[str, dict]):
    if USE_DB:
        conn = _get_db_conn()
        cur = conn.cursor()
        for username, info in users.items():
            cur.execute("""
            INSERT INTO users (username, password, avatar, fetched_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (username) DO UPDATE
              SET password = EXCLUDED.password,
                  avatar = EXCLUDED.avatar,
                  fetched_at = EXCLUDED.fetched_at;
            """, (username, info.get("password",""), info.get("avatar",""), info.get("fetched_at") or None))
        conn.commit()
        cur.close(); conn.close()
        return
    save_json(USERS_FILE, users)

# --- INVITES ---
def load_invites() -> dict:
    if USE_DB:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT code, used FROM invites;")
        rows = cur.fetchall()
        cur.close(); conn.close()
        return {r["code"]: bool(r["used"]) for r in rows}
    return load_json(INVITES_FILE, {})

def save_invites(invites: dict):
    if USE_DB:
        conn = _get_db_conn()
        cur = conn.cursor()
        for code, used in invites.items():
            cur.execute("""
            INSERT INTO invites (code, used) VALUES (%s, %s)
            ON CONFLICT (code) DO UPDATE SET used = EXCLUDED.used;
            """, (code, bool(used)))
        conn.commit()
        cur.close(); conn.close()
        return
    save_json(INVITES_FILE, invites)

# --- SAVED_URLS ---
def load_saved_urls() -> List[dict]:
    if USE_DB:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT aweme_id, play_url, hd_url FROM saved_urls;")
        rows = cur.fetchall()
        cur.close(); conn.close()
        return [dict(r) for r in rows]
    return load_json(SAVED_URLS_FILE, [])

def save_saved_urls(urls: List[dict]):
    if USE_DB:
        conn = _get_db_conn()
        cur = conn.cursor()
        for entry in urls:
            cur.execute("""
            INSERT INTO saved_urls (aweme_id, play_url, hd_url) VALUES (%s, %s, %s)
            ON CONFLICT (aweme_id) DO UPDATE SET play_url = EXCLUDED.play_url, hd_url = EXCLUDED.hd_url;
            """, (entry.get("aweme_id"), entry.get("play_url"), entry.get("hd_url")))
        conn.commit()
        cur.close(); conn.close()
        return
    save_json(SAVED_URLS_FILE, urls)

# --- SAVED_USER_URLS ---
def load_saved_user_urls() -> List[dict]:
    if USE_DB:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, username, aweme_id, play_url, hd_url, images FROM saved_user_urls ORDER BY id DESC;")
        rows = cur.fetchall()
        cur.close(); conn.close()
        out = []
        for r in rows:
            out.append({"id": r["id"], "username": r["username"], "aweme_id": r["aweme_id"], "play_url": r["play_url"], "hd_url": r["hd_url"], "images": r["images"] or []})
        return out
    return load_json(SAVED_USER_URLS_FILE, [])

def save_saved_user_urls(urls: List[dict]):
    if USE_DB:
        conn = _get_db_conn()
        cur = conn.cursor()
        for entry in urls:
            cur.execute("""
            INSERT INTO saved_user_urls (username, aweme_id, play_url, hd_url, images)
            VALUES (%s, %s, %s, %s, %s)
            """, (entry.get("username"), entry.get("aweme_id"), entry.get("play_url"), entry.get("hd_url"), json.dumps(entry.get("images") or [])))
        conn.commit()
        cur.close(); conn.close()
        return
    save_json(SAVED_USER_URLS_FILE, urls)

# --- POSTS ---
def load_posts() -> Dict[str, list]:
    if USE_DB:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT username, aweme_id, data FROM posts;")
        rows = cur.fetchall()
        cur.close(); conn.close()
        posts: Dict[str, list] = {}
        for r in rows:
            posts.setdefault(r["username"], []).append(r["data"])
        return posts
    return load_json(POSTS_FILE, {})

def save_posts(posts: Dict[str, list]):
    if USE_DB:
        conn = _get_db_conn()
        cur = conn.cursor()
        for username, ups in posts.items():
            for p in ups:
                aweme_id = p.get("aweme_id")
                cur.execute("""
                INSERT INTO posts (username, aweme_id, data)
                VALUES (%s, %s, %s)
                ON CONFLICT (username, aweme_id) DO UPDATE SET data = EXCLUDED.data;
                """, (username, aweme_id, json.dumps(p)))
        conn.commit()
        cur.close(); conn.close()
        return
    save_json(POSTS_FILE, posts)

# -------------------- Pydantic models --------------------
class RegisterIn(BaseModel):
    username: str
    password: str
    invite_code: str

class Token(BaseModel):
    access_token: str
    token_type: str

class URLIn(BaseModel):
    url: str

class SavedURL(BaseModel):
    aweme_id: str
    play_url: str
    hd_url: str

class UserIn(BaseModel):
    username: str

# New model for grouped slideshow payloads
class SlideshowIn(BaseModel):
    username: str
    aweme_id: str
    images: List[str]
    play_url: Optional[str] = None
    hd_url: Optional[str] = None

# -------------------- Utility --------------------
def now_iso():
    return datetime.utcnow().isoformat()

# -------------------- Profile monitor helper (unchanged) --------------------
def get_page(username: str) -> Optional[requests.Response]:
    try:
        url = username
        if not username.startswith("http"):
            url = f"https://www.tiktok.com/@{username}"
        r = requests.get(url, headers=WEB_HEADERS, timeout=10)
        return r
    except Exception:
        return None

def get_json_from_script(html: str, script_id_or_key: str) -> Optional[str]:
    try:
        m = re.search(rf'<script[^>]*id=[\'"]{re.escape(script_id_or_key)}[\'"][^>]*>([\s\S]*?)</script>', html, flags=re.I)
        if m:
            return m.group(1)
        m2 = re.search(rf'window\.{re.escape(script_id_or_key)}\s*=\s*({{[\s\S]*?}});', html, flags=re.I)
        if m2:
            return m2.group(1)
    except Exception:
        pass
    return None

def fetch_profile_info_for_monitor(username: str) -> Optional[Dict[str, Any]]:
    try:
        page = get_page(username)
        if not page or page.status_code != 200:
            return None
        try:
            json_text = get_json_from_script(page.text, "__UNIVERSAL_DATA_FOR_REHYDRATION__") or get_json_from_script(page.text, "__INIT_PROPS__") or get_json_from_script(page.text, "__NEXT_DATA__")
            data = json.loads(json_text) if json_text else {}
            user_detail = (data.get("__DEFAULT_SCOPE__", {}) .get("webapp.user-detail")
                           or data.get("__DEFAULT_SCOPE__", {}).get("webapp.userDetail")
                           or {})
            user_info = None
            if isinstance(user_detail, dict) and user_detail.get("userInfo"):
                ui = user_detail["userInfo"]
                user_info = ui.get("user") or ui
            if not user_info:
                for v in data.values():
                    if isinstance(v, dict) and "userInfo" in v:
                        ui = v["userInfo"]
                        user_info = ui.get("user") or ui
                        break
            avatar = None
            has_stories = False
            if user_info:
                avatar = None
                if isinstance(user_info.get("avatar_larger", {}).get("url_list"), list):
                    avatar = user_info.get("avatar_larger", {}).get("url_list")[-1]
                avatar = avatar or user_info.get("avatarLarger") or user_info.get("avatar_larger") or user_info.get("avatar")
                if isinstance(avatar, list):
                    avatar = avatar[-1] if avatar else None
                has_stories = bool(user_info.get("UserStoryStatus") or user_info.get("story_status") or user_info.get("hasStory") or False)
            return {"avatar_url": avatar, "has_stories": bool(has_stories)}
        except Exception:
            try:
                m = re.search(r'"avatarLarger"\s*:\s*"([^"]+)"', page.text)
                avatar = m.group(1) if m else None
                has_stories = bool(re.search(r'"story"', page.text, re.IGNORECASE))
                return {"avatar_url": avatar, "has_stories": has_stories}
            except Exception:
                logging.exception("Failed parse fallback for profile page for %s", username)
                return None
    except Exception:
        logging.exception("Failed to fetch profile info for monitor: %s", username)
        return None

# -------------------- tikwm submit helper (unchanged) --------------------
def submit_tikwm_task(video_id: str, max_submit_attempts: int = 5, poll_attempts: int = 120, poll_interval: float = 1.0):
    submit_url = "https://tikwm.com/api/video/task/submit"
    result_base = "https://tikwm.com/api/video/task/result?task_id="
    headers = {"User-Agent": WEB_HEADERS["User-Agent"]}
    task_id = None
    for attempt in range(max_submit_attempts):
        try:
            resp = requests.post(submit_url, data={"web": 1, "url": video_id}, headers=headers, timeout=15)
            resp.raise_for_status()
            j = resp.json()
            code = j.get("code")
            msg = j.get("msg", "")
            if code == 0 and "data" in j and j["data"].get("task_id"):
                task_id = j["data"]["task_id"]
                break
            logging.debug("submit_tikwm_task submit attempt %s returned code=%s msg=%s", attempt + 1, code, msg)
        except Exception as e:
            logging.debug("submit_tikwm_task submit attempt %s failed: %s", attempt + 1, e)
        time.sleep(2)
    if not task_id:
        logging.warning("submit_tikwm_task: failed to obtain task_id for video %s", video_id)
        return None
    for i in range(poll_attempts):
        try:
            r = requests.get(result_base + str(task_id), headers=headers, timeout=15)
            if r.status_code != 200:
                time.sleep(poll_interval)
                continue
            j2 = r.json()
            if j2.get("code") == 0 and isinstance(j2.get("data"), dict):
                status = j2["data"].get("status")
                if status == 2:
                    detail = j2["data"].get("detail", {})
                    play_url = detail.get("play_url") or detail.get("url")
                    size = detail.get("size")
                    cap = detail.get("title") or detail.get("desc") or detail.get("text") or ""
                    if cap:
                        try:
                            LATEST_TIKWM_CAPTION[video_id] = str(cap)
                        except Exception:
                            pass
                    if play_url:
                        logging.debug("submit_tikwm_task: got play_url for %s", video_id)
                        return {"play_url": play_url, "size": size, "title": cap}
                    else:
                        logging.debug("submit_tikwm_task: result had no play_url yet for %s (detail=%s)", video_id, detail)
                elif status == 3:
                    logging.warning("submit_tikwm_task: task failed for %s (status=3)", video_id)
                    return None
            time.sleep(poll_interval)
        except Exception as e:
            logging.debug("submit_tikwm_task poll error for %s: %s", video_id, e)
            time.sleep(poll_interval)
    logging.warning("submit_tikwm_task: timed out polling task result for %s", video_id)
    return None

# -------------------- helper: fetch_user_posts_tikwm (fetch up to max_items; handles pagination) --------------------
def fetch_user_posts_tikwm(username: str, max_items: int = 100, per_page: int = 50, timeout: int = 10) -> List[dict]:
    """
    Fetch posts for a username using tikwm's user/posts endpoint, paging until we have up to max_items.
    Uses params count and cursor. Returns list of video dicts (as returned by tikwm).
    """
    out = []
    cursor = 0
    attempts = 0
    max_attempts = 5
    while len(out) < max_items:
        params = {"unique_id": username, "count": per_page, "cursor": cursor}
        try:
            r = requests.get("https://www.tikwm.com/api/user/posts", params=params, headers=WEB_HEADERS, timeout=timeout)
            if r.status_code != 200:
                attempts += 1
                logging.debug("fetch_user_posts_tikwm: non-200 status %s for %s (attempt %d)", r.status_code, username, attempts)
                if attempts >= max_attempts:
                    break
                time.sleep(1 + attempts)
                continue
            j = r.json()
            if j.get("msg") != "success":
                logging.debug("fetch_user_posts_tikwm: msg != success for %s: %s", username, j.get("msg"))
                break
            vids = j.get("data", {}).get("videos", []) or []
            if not vids:
                break
            for v in vids:
                out.append(v)
                if len(out) >= max_items:
                    break
            has_more = bool(j.get("data", {}).get("has_more", False))
            cursor = j.get("data", {}).get("cursor", cursor)
            # if not has_more, break
            if not has_more:
                break
            # reset attempts on success
            attempts = 0
            # tiny sleep to be nice
            time.sleep(0.2)
        except Exception as e:
            attempts += 1
            logging.debug("fetch_user_posts_tikwm error for %s attempt %d: %s", username, attempts, e)
            if attempts >= max_attempts:
                break
            time.sleep(1 + attempts)
    # dedupe by video_id
    seen = set()
    normalized = []
    for v in out:
        vid = v.get("video_id") or v.get("aweme_id") or (v.get("id") if isinstance(v.get("id"), (str, int)) else None)
        if not vid:
            # try nested shapes
            if isinstance(v, dict):
                vid = v.get("video", {}).get("id") or v.get("aweme", {}).get("aweme_id") or v.get("aweme", {}).get("id")
        if vid:
            vid = str(vid)
            if vid in seen:
                continue
            seen.add(vid)
            normalized.append(v)
        else:
            # include raw if we can't extract id (edge cases)
            normalized.append(v)
        if len(normalized) >= max_items:
            break
    return normalized[:max_items]

# -------------------- App startup: init DB --------------------
@app.on_event("startup")
async def startup_event():
    _init_db()
    if USE_DB:
        try:
            conn = _get_db_conn()
            cur = conn.cursor()
            cur.execute("SELECT aweme_id, hd_url FROM saved_urls WHERE hd_url IS NOT NULL;")
            rows = cur.fetchall()
            for r in rows:
                if r.get("aweme_id") and r.get("hd_url"):
                    HD_URLS[r["aweme_id"]] = r["hd_url"]
            cur.close(); conn.close()
        except Exception as e:
            logging.debug("Failed to load hd urls into memory: %s", e)

# -------------------- Routes --------------------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request, q: str = None, type: str = Query(None, regex="^(latest|top)?$")):
    users = load_users()
    posts = load_posts()

    # multiple usernames (comma separated) support
    if q:
        usernames = [u.strip() for u in q.split(",") if u.strip()]
        if len(usernames) > 1:
            saved_user_urls = load_saved_user_urls()
            for uname in usernames:
                if uname not in users:
                    info = fetch_profile_info_for_monitor(uname) or {}
                    avatar = info.get("avatar_url") or DEFAULT_AVATAR
                    users[uname] = {"password": "", "avatar": avatar, "fetched_at": now_iso()}
                else:
                    users[uname]["fetched_at"] = now_iso()

                # fetch up to 100 posts per user using the robust helper
                try:
                    vids = fetch_user_posts_tikwm(uname, max_items=100, per_page=50)
                except Exception:
                    vids = []

                for v in vids:
                    vid = v.get("video_id") or v.get("aweme_id") or (v.get("id") if isinstance(v.get("id"), (str, int)) else None)
                    if not vid:
                        continue
                    vid = str(vid)
                    play = f"https://www.tikwm.com/video/media/play/{vid}.mp4"
                    hd = f"https://www.tikwm.com/video/media/hdplay/{vid}.mp4"
                    imgs = v.get("images", []) or []
                    entry = {
                        "username": uname,
                        "aweme_id": vid,
                        "play_url": play,
                        "hd_url": hd,
                        "images": imgs
                    }
                    if not any(u["username"] == uname and u["aweme_id"] == vid for u in saved_user_urls):
                        saved_user_urls.insert(0, entry)

            save_users(users)
            save_saved_user_urls(saved_user_urls)

            return templates.TemplateResponse("index.html", {
                "request": request,
                "users": users,
                "user_videos": [],
                "hd_urls": HD_URLS,
                "active_q": q,
                "view_type": type or "",
            })

    # single user logic (fetch up to 100)
    if q and "," not in (q or ""):
        uname = q
        if uname not in users:
            info = fetch_profile_info_for_monitor(uname) or {}
            users[uname] = {"password": "", "avatar": info.get("avatar_url") or DEFAULT_AVATAR, "fetched_at": now_iso()}
        else:
            users[uname]["fetched_at"] = now_iso()
        posts[uname] = []

        # Use the robust paging helper to fetch up to 100 videos per user
        try:
            all_posts = fetch_user_posts_tikwm(uname, max_items=100, per_page=50)
        except Exception:
            all_posts = []

        for v in all_posts:
            vid = v.get("video_id") or v.get("aweme_id") or (v.get("id") if isinstance(v.get("id"), (str, int)) else None)
            if not vid:
                continue
            vid = str(vid)
            HD_URLS[vid] = f"https://www.tikwm.com/video/media/hdplay/{vid}.mp4"
            posts[uname].append({
                "aweme_id": vid,
                "text": v.get("title", "") or v.get("desc", "") or "",
                "cover": v.get("cover", "") or v.get("cover", {}).get("url_list", [None])[-1] if isinstance(v.get("cover"), dict) else v.get("cover", ""),
                "play_url": f"https://www.tikwm.com/video/media/play/{vid}.mp4",
                "play_count": 0,
                "images": v.get("images", []) or []
            })
        save_posts(posts)
        save_users(users)

    # choose videos to render
    videos = []
    if type == "latest":
        for ups in posts.values():
            if ups:
                videos.append(ups[0])
    elif type == "top":
        top_list = [max(ups, key=lambda p: p.get("play_count", 0)) for ups in posts.values() if ups]
        videos = sorted(top_list, key=lambda p: p.get("play_count", 0), reverse=True)[:50]
    elif q:
        videos = posts.get(q, [])

    return templates.TemplateResponse("index.html", {
        "request": request,
        "users": users,
        "user_videos": videos,
        "hd_urls": HD_URLS,
        "active_q": q or "",
        "view_type": type or "",
    })

@app.get("/download")
async def download(video_id: str, hd: int = 0):
    posts = load_posts()
    found = None
    for ups in posts.values():
        for p in ups:
            if p["aweme_id"] == video_id:
                found = p
                break
        if found:
            break
    if not found:
        raise HTTPException(404, "Video not found")

    if hd and not HD_URLS.get(video_id):
        task_res = submit_tikwm_task(video_id)
        if task_res and task_res.get("play_url"):
            HD_URLS[video_id] = task_res["play_url"]
            saved = load_saved_urls()
            saved = [u for u in saved if u.get("aweme_id") != video_id]
            saved.insert(0, {"aweme_id": video_id, "play_url": found["play_url"], "hd_url": HD_URLS[video_id]})
            save_saved_urls(saved)

    url = HD_URLS.get(video_id) if hd else found["play_url"]
    r = requests.get(url, headers=WEB_HEADERS, timeout=15, stream=True)
    if r.status_code != 200:
        raise HTTPException(r.status_code, "Failed to fetch video")

    found["play_count"] = found.get("play_count", 0) + 1
    save_posts(posts)

    fname = f"{video_id}{'_HD' if hd else ''}.mp4"
    return StreamingResponse(r.raw, media_type="video/mp4",
                             headers={"Content-Disposition": f'attachment; filename="{fname}"'})

@app.post("/api/invite-code", status_code=201)
async def generate_invite_code():
    invites = load_invites()
    code = secrets.token_urlsafe(8)
    invites[code] = False
    save_invites(invites)
    return {"invite_code": code}

@app.post("/api/register", status_code=201)
async def register(data: RegisterIn):
    invites = load_invites()
    if data.invite_code not in invites or invites[data.invite_code]:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid or used invite code")
    invites[data.invite_code] = True
    save_invites(invites)

    users = load_users()
    if data.username in users and users[data.username].get("password"):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Username already registered")

    profile_info = fetch_profile_info_for_monitor(data.username) or {}
    avatar_url = profile_info.get("avatar_url") or DEFAULT_AVATAR

    users[data.username] = {
        "password": pwd_context.hash(data.password),
        "avatar": avatar_url,
        "fetched_at": now_iso()
    }
    save_users(users)
    return {"msg": "Registered"}

@app.post("/api/login", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    users = load_users()
    user = users.get(form_data.username)
    if not user or not pwd_context.verify(form_data.password, user.get("password", "")):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED,
                            "Incorrect username or password",
                            headers={"WWW-Authenticate": "Bearer"})
    token = jwt.encode(
        {"sub": form_data.username,
         "exp": datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)},
         SECRET_KEY, algorithm=ALGORITHM
    )
    return {"access_token": token, "token_type": "bearer"}

@app.get("/api/users")
async def api_users():
    users = load_users()
    return JSONResponse([
        {"username": u, "avatar": users[u].get("avatar", ""), "fetched_at": users[u].get("fetched_at", "")}
        for u in users
    ])

@app.delete("/api/users/{username}", status_code=204)
async def delete_user(username: str):
    users = load_users()
    if username in users:
        del users[username]
        save_users(users)
    return JSONResponse(status_code=204, content={})

@app.get("/api/latest")
async def api_latest():
    posts = load_posts()
    users = load_users()
    result = []
    for user, ups in posts.items():
        if ups:
            p = ups[0]
            result.append({
                "aweme_id": p["aweme_id"],
                "text": p.get("text", ""),
                "cover": p.get("cover", ""),
                "play_url": p.get("play_url", ""),
                "hd_url": HD_URLS.get(p["aweme_id"], ""),
                "username": user,
                "avatar": users.get(user, {}).get("avatar", "")
            })
    return JSONResponse(result)

@app.get("/api/top")
async def api_top(limit: int = 20):
    posts = load_posts()
    users = load_users()
    top = []
    for ups in posts.values():
        if ups:
            top.append(max(ups, key=lambda p: p.get("play_count", 0)))
    top = sorted(top, key=lambda p: p.get("play_count", 0), reverse=True)[:limit]
    out = []
    for pc in top:
        username = next(u for u, ups in posts.items() if pc in ups)
        out.append({
            "aweme_id": pc["aweme_id"],
            "text": pc.get("text", ""),
            "cover": pc.get("cover", ""),
            "play_url": pc.get("play_url", ""),
            "hd_url": HD_URLS.get(pc["aweme_id"], ""),
            "username": username,
            "avatar": load_users().get(username, {}).get("avatar", ""),
            "play_count": pc.get("play_count", 0)
        })
    return JSONResponse(out)

@app.post("/api/view/{video_id}")
async def api_view(video_id: str):
    posts = load_posts()
    for ups in posts.values():
        for p in ups:
            if p["aweme_id"] == video_id:
                p["play_count"] = p.get("play_count", 0) + 1
                save_posts(posts)
                return {"play_count": p["play_count"]}
    raise HTTPException(404, "Video not found")

@app.post("/api/from-url")
async def from_url(payload: URLIn):
    try:
        r = requests.get(payload.url, headers=WEB_HEADERS, timeout=10, allow_redirects=True)
        final = r.url
    except Exception:
        raise HTTPException(400, "Failed to resolve URL")
    parts = [p for p in urlparse(final).path.split("/") if p]
    aweme_id = None
    for i, p in enumerate(parts):
        if p == "video" and i + 1 < len(parts):
            aweme_id = parts[i + 1]
            break
    if not aweme_id:
        raise HTTPException(400, "Could not extract video ID from URL")

    task_result = submit_tikwm_task(final)
    if task_result and task_result.get("play_url"):
        play_url = task_result["play_url"]
        hd_url = play_url
        HD_URLS[aweme_id] = hd_url
        saved = load_saved_urls()
        saved = [u for u in saved if u.get("aweme_id") != aweme_id]
        saved.insert(0, {"aweme_id": aweme_id, "play_url": play_url, "hd_url": hd_url})
        save_saved_urls(saved)
    else:
        play_url = f"https://www.tikwm.com/video/media/play/{aweme_id}.mp4"
        hd_url = f"https://www.tikwm.com/video/media/hdplay/{aweme_id}.mp4"
        HD_URLS[aweme_id] = hd_url

    try:
        info = requests.get(f"https://www.tikwm.com/api/?url={final}&hd=1", headers=WEB_HEADERS, timeout=10).json()
        images = info.get("data", {}).get("images", [])
    except Exception:
        images = []

    return JSONResponse({
        "aweme_id": aweme_id,
        "play_url": play_url,
        "hd_url": hd_url,
        "images": images
    })

@app.get("/api/saved-urls")
async def get_saved_urls():
    return JSONResponse(load_saved_urls())

@app.post("/api/saved-urls", status_code=201)
async def post_saved_url(url_data: SavedURL):
    saved = load_saved_urls()
    if not any(u["aweme_id"] == url_data.aweme_id for u in saved):
        saved.insert(0, url_data.dict())
        save_saved_urls(saved)
    return JSONResponse(url_data.dict())

@app.delete("/api/saved-urls/{aweme_id}", status_code=204)
async def delete_saved_url(aweme_id: str):
    saved = load_saved_urls()
    saved = [u for u in saved if u["aweme_id"] != aweme_id]
    save_saved_urls(saved)
    return JSONResponse(status_code=204, content={})

@app.get("/api/saved-users")
async def get_saved_users():
    users = load_users()
    return JSONResponse([
        {"username": u, "avatar": users[u].get("avatar", ""), "fetched_at": users[u].get("fetched_at", "")}
        for u in users
    ])

@app.post("/api/saved-users", status_code=201)
async def post_saved_user(u: UserIn):
    users = load_users()
    if u.username not in users:
        profile_info = fetch_profile_info_for_monitor(u.username) or {}
        users[u.username] = {"password": "", "avatar": profile_info.get("avatar_url") or DEFAULT_AVATAR, "fetched_at": now_iso()}
        save_users(users)
    return JSONResponse({"username": u.username})

@app.delete("/api/saved-users/{username}", status_code=204)
async def delete_saved_user(username: str):
    users = load_users()
    if username in users:
        del users[username]
        save_users(users)
    return JSONResponse(status_code=204, content={})

# -------------------- Slideshow endpoints (grouped) --------------------
@app.get("/api/slideshow")
async def get_slideshow():
    """
    Return a list of slideshows grouped by (username, aweme_id).
    Each slideshow object contains: username, aweme_id, play_url, hd_url, images (list).
    This prevents returning a flat list of single-image entries and lets the frontend
    consume slideshows as a coherent viewer unit.
    """
    grouped = {}
    for entry in load_saved_user_urls():
        uname = entry.get("username", "")
        aid = entry.get("aweme_id", "")
        if not uname or not aid:
            continue
        key = (uname, aid)
        if key not in grouped:
            grouped[key] = {
                "username": uname,
                "aweme_id": aid,
                "play_url": entry.get("play_url", "") or "",
                "hd_url": entry.get("hd_url", "") or "",
                "images": list(entry.get("images", []) or [])
            }
        else:
            # merge images without duplicates, preserve order
            existing = grouped[key]["images"]
            for img in (entry.get("images", []) or []):
                if img not in existing:
                    existing.append(img)

        # prefer non-empty play/hd urls from entries
        if entry.get("play_url"):
            grouped[key]["play_url"] = entry.get("play_url")
        if entry.get("hd_url"):
            grouped[key]["hd_url"] = entry.get("hd_url")

    slides = list(grouped.values())
    return JSONResponse(slides)

@app.post("/api/slideshow", status_code=201)
async def post_slideshow(payload: SlideshowIn):
    """
    Save a grouped slideshow (username + aweme_id -> images list).
    This will replace any existing saved_user_urls entry for the same username+aweme_id.
    """
    saved = load_saved_user_urls()
    # remove any existing entries for same username+aweme_id
    saved = [e for e in saved if not (e.get("username") == payload.username and e.get("aweme_id") == payload.aweme_id)]
    # insert new grouped entry at front
    new_entry = {
        "username": payload.username,
        "aweme_id": payload.aweme_id,
        "play_url": payload.play_url or "",
        "hd_url": payload.hd_url or "",
        "images": payload.images or []
    }
    saved.insert(0, new_entry)
    save_saved_user_urls(saved)
    return JSONResponse(new_entry)

@app.delete("/api/slideshow/{aweme_id}", status_code=204)
async def delete_slideshow(aweme_id: str, username: Optional[str] = Query(None)):
    """
    Delete slideshow entries matching aweme_id and optional username.
    If username omitted, delete all saved_user_urls with that aweme_id.
    """
    saved = load_saved_user_urls()
    if username:
        saved = [e for e in saved if not (e.get("aweme_id") == aweme_id and e.get("username") == username)]
    else:
        saved = [e for e in saved if e.get("aweme_id") != aweme_id]
    save_saved_user_urls(saved)
    return JSONResponse(status_code=204, content={})

# -------------------- Image helpers endpoints (unchanged) --------------------
@app.get("/api/images/username/{username}")
async def get_images_by_username(username: str):
    posts = load_posts()
    if username not in posts:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    result = []
    for p in posts[username]:
        vid = p.get("aweme_id")
        try:
            info = requests.get(f"https://www.tikwm.com/api/?url=https://www.tiktok.com/video/{vid}&hd=1", headers=WEB_HEADERS, timeout=10).json()
            imgs = info.get("data", {}).get("images", [])
        except Exception:
            imgs = []
        if imgs:
            result.append({"aweme_id": vid, "images": imgs})
    return JSONResponse(result)

@app.get("/api/images/url/{video_id}")
async def get_images_by_video(video_id: str):
    try:
        info = requests.get(f"https://www.tikwm.com/api/?url=https://www.tiktok.com/video/{video_id}&hd=1", headers=WEB_HEADERS, timeout=10).json()
        imgs = info.get("data", {}).get("images", [])
    except Exception:
        imgs = []
    return JSONResponse({"aweme_id": video_id, "images": imgs})

@app.get("/ping")
async def ping():
    return {"status": "alive"}

def start():
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

if __name__ == "__main__":
    start()
