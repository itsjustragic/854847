# app_with_postgres_fetch100.py
import os
import re
import time
import json
import secrets
import logging
import asyncio
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from urllib.parse import quote_plus
from fastapi import FastAPI, HTTPException, Request, status, Body, Query, Depends
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse, Response
from pydantic import BaseModel, HttpUrl, Field
from jose import jwt
from passlib.context import CryptContext
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.templating import Jinja2Templates

# ---------------- config ----------------
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")  # Set to protect endpoints. If empty, endpoints are open.
DEFAULT_INTERVAL = 60  # seconds

# Force pingers to run continuously (overrides provided max_runs and prevents automatic stopping)
FORCE_RUN_24_7 = True

# Default keep-alive pinger name and interval (used when no persisted pinger exists)
DEFAULT_KEEP_NAME = "keep-bopcentral"
DEFAULT_KEEP_URL = "https://bopcentral.onrender.com/"
DEFAULT_KEEP_INTERVAL = DEFAULT_INTERVAL  # seconds

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ping_api")

# -------------------- Config --------------------
DATA_DIR = os.getenv("DATA_DIR", "data")
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

PINGERS_FILE = os.path.join(DATA_DIR, "pingers.json")

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
DELETED_USERS_FILE = os.path.join(DATA_DIR, "deleted_users.json")

# Headers & defaults
WEB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BopCentral/1.0)",
    "Accept-Encoding": "identity"
}
DEFAULT_AVATAR = (
    "https://media.discordapp.net/attachments/1343576085098664020/"
    "1366204471633510530/IMG_20250427_190832_902.jpg?..."
)

# TikWM helper state (keeps latest caption)
LATEST_TIKWM_CAPTION: Dict[str, str] = {}
HD_URLS: Dict[str, str] = {}

# Logging (enable debug for local debugging)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

# Password hashing
pwd_context = CryptContext(schemes=["pbkdf2_sha256", "bcrypt"], deprecated="auto")

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

# Deleted users persistence
def load_deleted_users() -> List[str]:
    return load_json(DELETED_USERS_FILE, [])

def save_deleted_users(lst: List[str]):
    save_json(DELETED_USERS_FILE, lst)

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
        # Replace table contents to reflect the provided dict (so deletes persist)
        cur.execute("DELETE FROM users;")
        for username, info in users.items():
            cur.execute("""
            INSERT INTO users (username, password, avatar, fetched_at)
            VALUES (%s, %s, %s, %s);
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
        cur.execute("DELETE FROM invites;")
        for code, used in invites.items():
            cur.execute("""
            INSERT INTO invites (code, used) VALUES (%s, %s);
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
        cur.execute("DELETE FROM saved_urls;")
        for entry in urls:
            cur.execute("""
            INSERT INTO saved_urls (aweme_id, play_url, hd_url) VALUES (%s, %s, %s);
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
        # Replace table contents so deletions persist
        cur.execute("DELETE FROM saved_user_urls;")
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
        # Replace posts table contents to reflect provided posts structure
        cur.execute("DELETE FROM posts;")
        for username, ups in posts.items():
            for p in ups:
                aweme_id = p.get("aweme_id")
                cur.execute("""
                INSERT INTO posts (username, aweme_id, data)
                VALUES (%s, %s, %s);
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
            if not has_more:
                break
            attempts = 0
            time.sleep(0.2)
        except Exception as e:
            attempts += 1
            logging.debug("fetch_user_posts_tikwm error for %s attempt %d: %s", username, attempts, e)
            if attempts >= max_attempts:
                break
            time.sleep(1 + attempts)
    seen = set()
    normalized = []
    for v in out:
        vid = v.get("video_id") or v.get("aweme_id") or (v.get("id") if isinstance(v.get("id"), (str, int)) else None)
        if not vid:
            if isinstance(v, dict):
                vid = v.get("video", {}).get("id") or v.get("aweme", {}).get("aweme_id") or v.get("aweme", {}).get("id")
        if vid:
            vid = str(vid)
            if vid in seen:
                continue
            seen.add(vid)
            normalized.append(v)
        else:
            normalized.append(v)
        if len(normalized) >= max_items:
            break
    return normalized[:max_items]

# --- Add these routes somewhere after your other /api routes (e.g. after /api/saved-urls) ---

@app.get("/api/saved-user-urls")
async def api_get_saved_user_urls():
    try:
        return JSONResponse(load_saved_user_urls())
    except Exception as e:
        logging.exception("GET /api/saved-user-urls failed")
        raise HTTPException(status_code=500, detail="Failed to load saved user urls")

@app.post("/api/saved-user-urls", status_code=201)
async def api_post_saved_user_urls(payload: list[dict] = Body(...)):
    if not isinstance(payload, list):
        payload = [payload]

    saved = load_saved_user_urls()  # list of dicts
    existing_lookup = {}
    for i, e in enumerate(saved):
        key = ( (e.get("username") or "").lower(), str(e.get("aweme_id") or "") )
        if key not in existing_lookup:
            existing_lookup[key] = i

    inserted = 0
    for item in payload:
        uname = item.get("username") or ""
        aid = str(item.get("aweme_id") or "")
        key = (uname.lower(), aid)

        if key in existing_lookup:
            idx = existing_lookup[key]
            entry = saved[idx]
            existing_imgs = list(entry.get("images") or [])
            for img in (item.get("images") or []):
                if img and img not in existing_imgs:
                    existing_imgs.append(img)
            entry["images"] = existing_imgs
            if item.get("play_url"):
                entry["play_url"] = item.get("play_url")
            if item.get("hd_url"):
                entry["hd_url"] = item.get("hd_url")
            saved[idx] = entry
        else:
            new_entry = {
                "username": uname,
                "aweme_id": aid,
                "play_url": item.get("play_url") or "",
                "hd_url": item.get("hd_url") or "",
                "images": list(item.get("images") or [])
            }
            saved.insert(0, new_entry)
            existing_lookup = {k: (v + 1) for k, v in existing_lookup.items()}
            existing_lookup[key] = 0
            inserted += 1

    try:
        save_saved_user_urls(saved)
    except Exception:
        logging.exception("Failed saving saved-user-urls after POST")
        raise HTTPException(status_code=500, detail="Failed to persist saved_user_urls")

    return JSONResponse({"inserted": inserted, "total": len(saved)})

@app.delete("/api/saved-user-urls/{aweme_id}", status_code=204)
async def api_delete_saved_user_urls(aweme_id: str, username: Optional[str] = Query(None)):
    saved = load_saved_user_urls()
    if username:
        saved = [e for e in saved if not (str(e.get("aweme_id")) == str(aweme_id) and (e.get("username") or "").lower() == username.lower())]
    else:
        saved = [e for e in saved if str(e.get("aweme_id")) != str(aweme_id)]
    save_saved_user_urls(saved)
    return Response(status_code=204)

# -------------------- App startup: init DB --------------------
@app.on_event("startup")
async def startup_event():
    _init_db()
    # Load existing hd urls into memory
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

    # --- Load persisted pingers and restart them automatically ---
    try:
        defs = load_json(PINGERS_FILE, [])
        started = 0
        if isinstance(defs, list):
            async with PINGERS_LOCK:
                for d in defs:
                    name = d.get("name")
                    url = d.get("url")
                    interval = int(d.get("interval") or DEFAULT_INTERVAL)
                    max_runs = d.get("max_runs")
                    # Force continuous operation if configured
                    if FORCE_RUN_24_7:
                        max_runs = None
                    if not name or not url:
                        continue
                    if name in PINGERS:
                        continue
                    meta = PingerMeta(
                        name=name,
                        url=url,
                        interval=interval,
                        started_at=d.get("started_at") or datetime.utcnow().isoformat() + "Z",
                        max_runs=max_runs
                    )
                    task = asyncio.create_task(pinger_loop(name=name, url=url, interval=interval, max_runs=max_runs))
                    PINGERS[name] = {"meta": meta, "task": task}
                    started += 1
        logging.info("Startup: restarted %d persisted pinger(s)", started)

        # If FORCE_RUN_24_7 and no pinger for bopcentral exists, create one now
        if FORCE_RUN_24_7:
            async with PINGERS_LOCK:
                if DEFAULT_KEEP_NAME not in PINGERS:
                    meta = PingerMeta(
                        name=DEFAULT_KEEP_NAME,
                        url=DEFAULT_KEEP_URL,
                        interval=DEFAULT_KEEP_INTERVAL,
                        started_at=datetime.utcnow().isoformat() + "Z",
                        max_runs=None
                    )
                    task = asyncio.create_task(pinger_loop(name=DEFAULT_KEEP_NAME, url=DEFAULT_KEEP_URL, interval=DEFAULT_KEEP_INTERVAL, max_runs=None))
                    PINGERS[DEFAULT_KEEP_NAME] = {"meta": meta, "task": task}
                    # persist the default pinger to file (ensures restart next time)
                    try:
                        persist_pingers_to_file()
                    except Exception:
                        logging.exception("Failed to persist default pinger after startup")
                    logging.info("Startup: created default persistent pinger '%s' -> %s every %ss", DEFAULT_KEEP_NAME, DEFAULT_KEEP_URL, DEFAULT_KEEP_INTERVAL)
    except Exception:
        logging.exception("Failed to restart persisted pingers on startup")

    # Start the watchdog monitor that ensures persisted pingers always have running tasks
    try:
        # Create and store the monitor task so we can optionally cancel it during shutdown
        global PINGERS_MONITOR_TASK
        PINGERS_MONITOR_TASK = asyncio.create_task(pingers_watchdog())
        logging.info("Pingers watchdog started")
    except Exception:
        logging.exception("Failed to start pingers_watchdog")

# -------------------- Routes --------------------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request, q: str = None, type: str = Query(None, regex="^(latest|top)?$")):
    users = load_users()
    posts = load_posts()
    deleted = set(load_deleted_users())

    # multiple usernames (comma separated) support
    if q:
        usernames = [u.strip() for u in q.split(",") if u.strip()]
        if len(usernames) > 1:
            saved_user_urls = load_saved_user_urls()
            for uname in usernames:
                # If user is blacklisted (deleted), do not auto-add
                if uname in deleted:
                    logging.debug("index: skipping auto-add of blacklisted user %s", uname)
                    continue

                if uname not in users:
                    info = fetch_profile_info_for_monitor(uname) or {}
                    avatar = info.get("avatar_url") or DEFAULT_AVATAR
                    users[uname] = {"password": "", "avatar": avatar, "fetched_at": now_iso()}
                else:
                    users[uname]["fetched_at"] = now_iso()

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
        deleted = set(load_deleted_users())
        if uname in deleted:
            # Do not auto-add or fetch posts if user is blacklisted
            logging.debug("index: single-user view requested for deleted/blacklisted user %s - not auto-adding", uname)
            posts[uname] = []
        else:
            if uname not in users:
                info = fetch_profile_info_for_monitor(uname) or {}
                users[uname] = {"password": "", "avatar": info.get("avatar_url") or DEFAULT_AVATAR, "fetched_at": now_iso()}
            else:
                users[uname]["fetched_at"] = now_iso()
            posts[uname] = []

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

    # If HD requested, only use submit_tikwm_task (no fallback). Try up to 3 attempts.
    if hd:
        if not HD_URLS.get(video_id):
            task_res = None
            for attempt in range(3):
                try:
                    task_res = submit_tikwm_task(video_id)
                    if task_res and task_res.get("play_url"):
                        HD_URLS[video_id] = task_res["play_url"]
                        try:
                            saved = load_saved_urls()
                            saved = [u for u in saved if u.get("aweme_id") != video_id]
                            saved.insert(0, {"aweme_id": video_id, "play_url": found["play_url"], "hd_url": HD_URLS[video_id]})
                            save_saved_urls(saved)
                        except Exception:
                            logging.exception("Failed to persist hd_url after tikwm submit for %s", video_id)
                        logging.debug("download: obtained HD URL for %s on attempt %s", video_id, attempt + 1)
                        break
                    else:
                        logging.debug("download: submit_tikwm_task did not return play_url for %s on attempt %s", video_id, attempt + 1)
                except Exception as e:
                    logging.debug("download: submit_tikwm_task attempt %s failed for %s: %s", attempt + 1, video_id, e)
                time.sleep(1)
            # If after attempts we still don't have an HD url, fail — NO FALLBACK
            if not HD_URLS.get(video_id):
                logging.warning("download: failed to obtain HD via tikwm for %s after 3 attempts", video_id)
                raise HTTPException(status_code=503, detail="Failed to obtain HD via tikwm")

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

    # If this user was previously deleted (blacklisted), un-blacklist them on explicit register
    deleted = set(load_deleted_users())
    if data.username in deleted:
        deleted.remove(data.username)
        save_deleted_users(list(deleted))

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
    deleted = set(load_deleted_users())
    # Do not expose deleted users in the list
    return JSONResponse([
        {"username": u, "avatar": users[u].get("avatar", ""), "fetched_at": users[u].get("fetched_at", "")}
        for u in users if u not in deleted
    ])

@app.delete("/api/users/{username}", status_code=204)
async def delete_user(username: str, request: Request = None):
    """
    Admin delete endpoint — also performs full removal + blacklist.
    If you are using the UI delete button, the UI calls /api/saved-users/{username} (which has also been updated),
    so both endpoints behave the same.
    """
    # If ADMIN_TOKEN is set, enforce it
    if ADMIN_TOKEN:
        token = (request.headers.get("X-Admin-Token","") if request else "")
        if token != ADMIN_TOKEN:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid admin token")

    # DB-mode: perform direct deletes so removals are persisted immediately
    if USE_DB:
        try:
            conn = _get_db_conn()
            cur = conn.cursor()
            cur.execute("DELETE FROM users WHERE username = %s;", (username,))
            cur.execute("DELETE FROM posts WHERE username = %s;", (username,))
            cur.execute("DELETE FROM saved_user_urls WHERE LOWER(username) = LOWER(%s);", (username,))
            conn.commit()
            cur.close(); conn.close()
        except Exception:
            logging.exception("DB-mode delete_user failed for %s", username)
            # fallthrough to file-mode fallback below so that deleted_users gets updated
    else:
        users = load_users()
        if username in users:
            del users[username]
            save_users(users)

        posts = load_posts()
        if username in posts:
            del posts[username]
            save_posts(posts)

        saved_user_urls = load_saved_user_urls()
        saved_user_urls = [e for e in saved_user_urls if (e.get("username") or "").lower() != username.lower()]
        save_saved_user_urls(saved_user_urls)

    # Add to deleted/blacklist so the app won't auto-recreate this user
    deleted = set(load_deleted_users())
    if username not in deleted:
        deleted.add(username)
        save_deleted_users(list(deleted))

    logging.info("User %s deleted and blacklisted (must be re-added explicitly to restore)", username)
    return Response(status_code=204)

@app.get("/api/latest")
async def api_latest():
    posts = load_posts()
    users = load_users()
    deleted = set(load_deleted_users())
    result = []
    for user, ups in posts.items():
        if user in deleted:
            continue
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
    deleted = set(load_deleted_users())
    top = []
    for ups_user, ups in posts.items():
        if ups_user in deleted:
            continue
        if ups:
            top.append(max(ups, key=lambda p: p.get("play_count", 0)))
    top = sorted(top, key=lambda p: p.get("play_count", 0), reverse=True)[:limit]
    out = []
    for pc in top:
        username = next(u for u, ups in posts.items() if pc in ups)
        if username in deleted:
            continue
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

# Helper: extract aweme id robustly
def extract_aweme_id_from_string(s: str) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    m = re.fullmatch(r"\d{5,}", s)
    if m:
        return m.group(0)
    m = re.search(r"/video/(\d+)", s)
    if m:
        return m.group(1)
    m = re.search(r"(\d{8,})", s)
    if m:
        return m.group(1)
    return None

@app.post("/api/from-url")
async def from_url(payload: URLIn):
    raw = (payload.url or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="Missing 'url' in request body")

    aweme_id = extract_aweme_id_from_string(raw)
    final_url = None

    try:
        if not raw.startswith("http"):
            if aweme_id:
                candidate = f"https://www.tiktok.com/video/{aweme_id}"
            else:
                candidate = raw
        else:
            candidate = raw

        try:
            r = requests.get(candidate, headers=WEB_HEADERS, timeout=10, allow_redirects=True)
            final_url = r.url or candidate
        except Exception:
            final_url = candidate
    except Exception:
        final_url = raw

    if not aweme_id:
        aweme_id = extract_aweme_id_from_string(final_url)

    if not aweme_id:
        detail_msg = f"Could not extract video ID from URL. tried: {final_url!s}"
        logging.debug("from_url extraction failed for raw=%s final=%s", raw, final_url)
        raise HTTPException(status_code=400, detail=detail_msg)

    canonical_video_url = f"https://www.tiktok.com/video/{aweme_id}"

    # ONLY use submit_tikwm_task for HD (no fallback). Try up to 3 attempts.
    task_res = None
    for attempt in range(3):
        try:
            task_res = submit_tikwm_task(canonical_video_url)
            if task_res and task_res.get("play_url"):
                break
        except Exception as e:
            logging.debug("from_url: submit_tikwm_task attempt %s failed for %s: %s", attempt + 1, canonical_video_url, e)
        time.sleep(1)

    if task_res and task_res.get("play_url"):
        play_url = task_res["play_url"]
        hd_url = play_url
        HD_URLS[aweme_id] = hd_url
        saved = load_saved_urls()
        saved = [u for u in saved if u.get("aweme_id") != aweme_id]
        saved.insert(0, {"aweme_id": aweme_id, "play_url": play_url, "hd_url": hd_url})
        save_saved_urls(saved)
    else:
        logging.warning("from_url: failed to obtain HD via tikwm for %s after 3 attempts", canonical_video_url)
        raise HTTPException(status_code=503, detail="Failed to obtain HD via tikwm")

    images = []
    try:
        info = requests.get(f"https://www.tikwm.com/api/?url={quote_plus(canonical_video_url)}", headers=WEB_HEADERS, timeout=10)
        if info.status_code == 200:
            j = info.json()
            images = j.get("data", {}).get("images", []) or []
    except Exception:
        logging.debug("from_url: failed to fetch images for %s", canonical_video_url)

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
    return Response(status_code=204)

@app.get("/api/saved-users")
async def get_saved_users():
    users = load_users()
    deleted = set(load_deleted_users())
    return JSONResponse([
        {"username": u, "avatar": users[u].get("avatar", ""), "fetched_at": users[u].get("fetched_at", "")}
        for u in users if u not in deleted
    ])

@app.post("/api/saved-users", status_code=201)
async def post_saved_user(u: UserIn):
    users = load_users()
    deleted = set(load_deleted_users())
    # If user was blacklisted, un-blacklist them on explicit save
    if u.username in deleted:
        deleted.remove(u.username)
        save_deleted_users(list(deleted))
    if u.username not in users:
        profile_info = fetch_profile_info_for_monitor(u.username) or {}
        users[u.username] = {"password": "", "avatar": profile_info.get("avatar_url") or DEFAULT_AVATAR, "fetched_at": now_iso()}
        save_users(users)
    return JSONResponse({"username": u.username})

@app.delete("/api/saved-users/{username}", status_code=204)
async def delete_saved_user(username: str):
    """
    This is the endpoint the frontend uses when you press the 'Delete' button on a saved user.
    It now performs a full delete:
      - remove from users.json / users table
      - remove posts[username]
      - remove saved_user_urls entries for the user
      - persist the username in deleted_users.json so it won't be auto-created
    """
    # DB-mode direct deletes
    if USE_DB:
        try:
            conn = _get_db_conn()
            cur = conn.cursor()
            cur.execute("DELETE FROM users WHERE username = %s;", (username,))
            cur.execute("DELETE FROM posts WHERE username = %s;", (username,))
            cur.execute("DELETE FROM saved_user_urls WHERE LOWER(username) = LOWER(%s);", (username,))
            conn.commit()
            cur.close(); conn.close()
        except Exception:
            logging.exception("DB-mode delete_saved_user failed for %s", username)
            # fallback to file-mode below
    else:
        users = load_users()
        if username in users:
            del users[username]
            save_users(users)

        posts = load_posts()
        if username in posts:
            del posts[username]
            save_posts(posts)

        saved_user_urls = load_saved_user_urls()
        saved_user_urls = [e for e in saved_user_urls if (e.get("username") or "").lower() != username.lower()]
        save_saved_user_urls(saved_user_urls)

    # Persist blacklist
    if username not in load_deleted_users():
        deleted = set(load_deleted_users())
        deleted.add(username)
        save_deleted_users(list(deleted))

    logging.info("Saved-user DELETE: user %s removed and blacklisted", username)
    return Response(status_code=204)

# -------------------- Slideshow endpoints (grouped) --------------------
@app.get("/api/slideshow")
async def get_slideshow():
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
            existing = grouped[key]["images"]
            for img in (entry.get("images", []) or []):
                if img not in existing:
                    existing.append(img)
        if entry.get("play_url"):
            grouped[key]["play_url"] = entry.get("play_url")
        if entry.get("hd_url"):
            grouped[key]["hd_url"] = entry.get("hd_url")
    slides = list(grouped.values())
    return JSONResponse(slides)

@app.post("/api/slideshow", status_code=201)
async def post_slideshow(payload: SlideshowIn):
    saved = load_saved_user_urls()
    saved = [e for e in saved if not (e.get("username") == payload.username and e.get("aweme_id") == payload.aweme_id)]
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
    saved = load_saved_user_urls()
    if username:
        saved = [e for e in saved if not (e.get("aweme_id") == aweme_id and e.get("username") == username)]
    else:
        saved = [e for e in saved if e.get("aweme_id") != aweme_id]
    save_saved_user_urls(saved)
    return Response(status_code=204)

# -------------------- Image helpers endpoints (removed &hd=1 usage) --------------------
@app.get("/api/images/username/{username}")
async def get_images_by_username(username: str):
    posts = load_posts()
    if username not in posts:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    result = []
    for p in posts[username]:
        vid = p.get("aweme_id")
        try:
            info = requests.get(f"https://www.tikwm.com/api/?url=https://www.tiktok.com/video/{vid}", headers=WEB_HEADERS, timeout=10).json()
            imgs = info.get("data", {}).get("images", [])
        except Exception:
            imgs = []
        if imgs:
            result.append({"aweme_id": vid, "images": imgs})
    return JSONResponse(result)

@app.get("/api/images/url/{video_id}")
async def get_images_by_video(video_id: str):
    try:
        info = requests.get(f"https://www.tikwm.com/api/?url=https://www.tiktok.com/video/{video_id}", headers=WEB_HEADERS, timeout=10).json()
        imgs = info.get("data", {}).get("images", [])
    except Exception:
        imgs = []
    return JSONResponse({"aweme_id": video_id, "images": imgs})

# ---------------- internal task store (unchanged) ----------------
class PingerMeta(BaseModel):
    name: str
    url: str
    interval: int
    started_at: str
    last_ping_at: Optional[str] = None
    runs: int = 0
    max_runs: Optional[int] = None
    status: str = "running"
    last_status_code: Optional[int] = None
    last_error: Optional[str] = None

PINGERS: Dict[str, Dict[str, Any]] = {}
PINGERS_LOCK = asyncio.Lock()

# Watchdog monitor task handle (set on startup)
PINGERS_MONITOR_TASK: Optional[asyncio.Task] = None

# ---------------- request models ----------------
class StartPingIn(BaseModel):
    url: HttpUrl
    name: Optional[str] = Field(None)
    interval: Optional[int] = Field(DEFAULT_INTERVAL, gt=0)
    max_runs: Optional[int] = Field(None, gt=0)

class StopPingIn(BaseModel):
    name: str

# ---------------- helpers ----------------
def check_admin_token(request: Request):
    if ADMIN_TOKEN:
        token = request.headers.get("X-Admin-Token", "")
        if not token or token != ADMIN_TOKEN:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid admin token")

async def safe_sleep(seconds: float):
    await asyncio.sleep(seconds)

# ---------------- pinger persistence helpers ----------------
def _serialize_meta(m: PingerMeta) -> dict:
    return {
        "name": m.name,
        "url": m.url,
        "interval": m.interval,
        "started_at": m.started_at,
        "max_runs": m.max_runs
    }

def persist_pingers_to_file():
    try:
        with open(PINGERS_FILE, "w", encoding="utf-8") as f:
            arr = []
            # Ensure we capture consistent snapshot
            for name, entry in list(PINGERS.items()):
                try:
                    meta: PingerMeta = entry["meta"]
                    arr.append(_serialize_meta(meta))
                except Exception:
                    continue
            json.dump(arr, f, indent=2, ensure_ascii=False)
    except Exception:
        logging.exception("Failed to persist pingers to file")

def remove_pinger_from_file(name: str):
    try:
        arr = load_json(PINGERS_FILE, [])
        if not isinstance(arr, list):
            arr = []
        arr = [d for d in arr if d.get("name") != name]
        save_json(PINGERS_FILE, arr)
    except Exception:
        logging.exception("Failed to remove pinger from persisted file: %s", name)

# pinger_loop (keeps same behavior but made more robust and persistent)
async def pinger_loop(name: str, url: str, interval: int, max_runs: Optional[int]):
    meta: PingerMeta = PINGERS[name]["meta"]
    import httpx
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            while True:
                # If pinger definition removed externally (and we removed from PINGERS), exit.
                if PINGERS.get(name) is None:
                    log.info("Pinger %s requested stop (entry removed)", name)
                    break
                start_ts = datetime.utcnow().isoformat() + "Z"
                try:
                    resp = await client.get(url)
                    status_code = resp.status_code
                    if status_code >= 400:
                        log.warning("Pinger %s got status %s for %s", name, status_code, url)
                    async with PINGERS_LOCK:
                        m: PingerMeta = PINGERS[name]["meta"]
                        m.last_ping_at = start_ts
                        m.runs += 1
                        m.last_status_code = status_code
                        m.last_error = None
                        m.status = "running"
                except asyncio.CancelledError:
                    log.info("Pinger %s cancelled during request", name)
                    # Do NOT set status to stopped here if FORCE_RUN_24_7 is True; tasks should be recreated on startup
                    break
                except Exception as e:
                    log.exception("Pinger %s error while pinging %s", name, url)
                    async with PINGERS_LOCK:
                        if name in PINGERS:
                            m: PingerMeta = PINGERS[name]["meta"]
                            m.last_ping_at = start_ts
                            m.runs += 1
                            m.last_status_code = None
                            m.last_error = repr(e)
                            m.status = "error"
                # Persist meta snapshot to file so the pinger is restartable
                try:
                    async with PINGERS_LOCK:
                        persist_pingers_to_file()
                except Exception:
                    logging.debug("Failed to persist pingers while running loop for %s", name)
                # If a max_runs was set originally, we intentionally ignore it when FORCE_RUN_24_7==True
                if (not FORCE_RUN_24_7) and max_runs:
                    async with PINGERS_LOCK:
                        if name not in PINGERS:
                            log.info("Pinger %s removed; exiting loop", name)
                            break
                        m: PingerMeta = PINGERS[name]["meta"]
                        if m.max_runs and m.runs >= m.max_runs:
                            m.status = "stopped"
                            log.info("Pinger %s reached max_runs (%s), stopping", name, m.max_runs)
                            break
                try:
                    await safe_sleep(interval)
                except asyncio.CancelledError:
                    log.info("Pinger %s cancelled during sleep", name)
                    break
        finally:
            async with PINGERS_LOCK:
                if name in PINGERS:
                    # keep meta visible but mark not running if task exited unexpectedly
                    try:
                        PINGERS[name]["meta"].status = PINGERS[name]["meta"].status or "stopped"
                    except Exception:
                        pass
            log.info("Pinger %s loop exited", name)

# -------------------- Watchdog monitor: ensures persisted pingers have running tasks --------------------
async def pingers_watchdog(check_interval: int = 30):
    """
    Background monitor that ensures persisted pinger definitions have running tasks.
    If a pinger task is missing or done, it recreates the task (and updates PINGERS).
    """
    logging.info("pingers_watchdog: starting (interval=%s)", check_interval)
    while True:
        try:
            # load persisted definitions once in this loop
            try:
                defs = load_json(PINGERS_FILE, [])
            except Exception:
                defs = []
            # Build a map from name -> def for persisted definitions
            persisted = {}
            if isinstance(defs, list):
                for d in defs:
                    n = d.get("name")
                    if not n:
                        continue
                    persisted[n] = d

            async with PINGERS_LOCK:
                # Ensure persisted definitions are present in PINGERS and running
                for name, d in persisted.items():
                    try:
                        url = d.get("url")
                        interval = int(d.get("interval") or DEFAULT_INTERVAL)
                        max_runs = d.get("max_runs")
                        if FORCE_RUN_24_7:
                            max_runs = None
                        if name in PINGERS:
                            task = PINGERS[name].get("task")
                            # If task missing or done, restart
                            if (not task) or (task.done()):
                                logging.warning("pingers_watchdog: task for '%s' missing/done — recreating", name)
                                meta = PINGERS[name]["meta"]
                                # ensure meta has the latest persisted interval/url
                                meta.url = url or meta.url
                                meta.interval = interval or meta.interval
                                meta.max_runs = max_runs
                                new_task = asyncio.create_task(pinger_loop(name=name, url=meta.url, interval=meta.interval, max_runs=meta.max_runs))
                                PINGERS[name]["task"] = new_task
                        else:
                            # not present: create it
                            logging.info("pingers_watchdog: creating missing pinger entry for '%s'", name)
                            meta = PingerMeta(
                                name=name,
                                url=url,
                                interval=interval,
                                started_at=d.get("started_at") or datetime.utcnow().isoformat() + "Z",
                                max_runs=max_runs
                            )
                            task = asyncio.create_task(pinger_loop(name=name, url=url, interval=interval, max_runs=max_runs))
                            PINGERS[name] = {"meta": meta, "task": task}
                    except Exception:
                        logging.exception("pingers_watchdog: failed to ensure persisted pinger %s", name)

                # If FORCE_RUN_24_7 is True, ensure default keep pinger exists
                if FORCE_RUN_24_7:
                    if DEFAULT_KEEP_NAME not in PINGERS:
                        try:
                            logging.info("pingers_watchdog: creating default keep pinger '%s'", DEFAULT_KEEP_NAME)
                            meta = PingerMeta(
                                name=DEFAULT_KEEP_NAME,
                                url=DEFAULT_KEEP_URL,
                                interval=DEFAULT_KEEP_INTERVAL,
                                started_at=datetime.utcnow().isoformat() + "Z",
                                max_runs=None
                            )
                            task = asyncio.create_task(pinger_loop(name=DEFAULT_KEEP_NAME, url=DEFAULT_KEEP_URL, interval=DEFAULT_KEEP_INTERVAL, max_runs=None))
                            PINGERS[DEFAULT_KEEP_NAME] = {"meta": meta, "task": task}
                            persist_pingers_to_file()
                        except Exception:
                            logging.exception("pingers_watchdog: failed to create default keep pinger")
                # Also, if there are entries in PINGERS that are not persisted (e.g. created by API), ensure they are persisted
                try:
                    persist_pingers_to_file()
                except Exception:
                    logging.debug("pingers_watchdog: persist failed")
        except asyncio.CancelledError:
            logging.info("pingers_watchdog: cancelled; exiting")
            break
        except Exception:
            logging.exception("pingers_watchdog: unexpected error")
        try:
            await asyncio.sleep(check_interval)
        except asyncio.CancelledError:
            logging.info("pingers_watchdog: cancelled during sleep; exiting")
            break

@app.post("/api/ping/start")
async def api_ping_start(payload: StartPingIn, request: Request):
    check_admin_token(request)
    name = payload.name or f"keep-{int(datetime.utcnow().timestamp())}"
    name = name.strip()
    interval = int(payload.interval or DEFAULT_INTERVAL)
    # ignore provided max_runs when forcing continuous operation
    max_runs = int(payload.max_runs) if payload.max_runs is not None else None
    if FORCE_RUN_24_7:
        max_runs = None
    url = str(payload.url)
    async with PINGERS_LOCK:
        if name in PINGERS:
            raise HTTPException(status_code=409, detail=f"Pinger with name '{name}' already exists")
        meta = PingerMeta(
            name=name,
            url=url,
            interval=interval,
            started_at=datetime.utcnow().isoformat() + "Z",
            max_runs=max_runs
        )
        task = asyncio.create_task(pinger_loop(name=name, url=url, interval=interval, max_runs=max_runs))
        PINGERS[name] = {"meta": meta, "task": task}
        # persist to file
        try:
            persist_pingers_to_file()
        except Exception:
            logging.exception("Failed to persist pingers after start")
    log.info("Started pinger '%s' -> %s every %ss (max_runs=%s)", name, url, interval, max_runs)
    return JSONResponse({"ok": True, "name": name, "url": url, "interval": interval, "max_runs": max_runs})

@app.post("/api/ping/stop")
async def api_ping_stop(payload: StopPingIn, request: Request):
    check_admin_token(request)
    name = payload.name.strip()
    async with PINGERS_LOCK:
        if name not in PINGERS:
            raise HTTPException(status_code=404, detail=f"No pinger named '{name}'")
        entry = PINGERS.pop(name)
        task: asyncio.Task = entry.get("task")
        # attempt to cancel the running task gracefully
        if task and not task.done():
            task.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
        except Exception:
            pass
        # remove from persisted file
        try:
            remove_pinger_from_file(name)
        except Exception:
            logging.exception("Failed to remove pinger from persisted file during stop: %s", name)
    log.info("Stopped and removed pinger '%s'", name)
    return JSONResponse({"ok": True, "name": name})

@app.get("/api/ping/status")
async def api_ping_status(request: Request):
    check_admin_token(request)
    async with PINGERS_LOCK:
        out = {name: PINGERS[name]["meta"].dict() for name in PINGERS}
    return JSONResponse(out)

@app.get("/api/ping/health")
async def api_ping_health():
    return {"status": "ok", "active_pingers": len(PINGERS)}

# ---------------- graceful shutdown ----------------
# NOTE: We intentionally do NOT cancel pinger tasks here. Tasks will be restarted on next startup
# because pinger definitions are persisted to disk. This avoids killing pings on FastAPI's shutdown.
@app.on_event("shutdown")
async def shutdown_event():
    log.info("Shutdown event: preserving %d pinger(s) for restart on next startup", len(PINGERS))
    # Attempt to cancel watchdog monitor task (optional)
    global PINGERS_MONITOR_TASK
    try:
        if PINGERS_MONITOR_TASK and not PINGERS_MONITOR_TASK.done():
            PINGERS_MONITOR_TASK.cancel()
            try:
                await asyncio.wait_for(PINGERS_MONITOR_TASK, timeout=2.0)
            except Exception:
                pass
    except Exception:
        logging.exception("Failed to cancel pingers_watchdog cleanly")

    # Do not cancel pinger tasks; they will die when process exits but persisted entries will be restarted on next startup.
    try:
        persist_pingers_to_file()
    except Exception:
        logging.exception("Failed to persist pingers during shutdown")

@app.get("/ping")
async def ping():
    return {"status": "alive"}

def start():
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

if __name__ == "__main__":
    start()
