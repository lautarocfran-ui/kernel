from __future__ import annotations

import asyncio
import math
import os
import random
import string
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

load_dotenv()

APP_NAME = "KERNEL // 4C LEET"
API_URL = "https://auth.roblox.com/v1/usernames/validate"

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
WORKERS = max(1, min(6, int(os.getenv("WORKERS", "3"))))
MIN_INTERVAL = float(os.getenv("MIN_INTERVAL", "0.55"))
MAX_INTERVAL = float(os.getenv("MAX_INTERVAL", "2.8"))

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
AVAILABLE_FILE = DATA_DIR / "available_4c.txt"
SEEN_FILE = DATA_DIR / "seen_4c.txt"

LETTERS = string.ascii_lowercase
DIGITS = string.digits

app = FastAPI(title=APP_NAME)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


class State:
    def __init__(self):
        self.running = False
        self.checked = 0
        self.available = 0
        self.rate_limited = False
        self.start_time = 0.0
        self.last_hit: Optional[str] = None
        self.last_status = "IDLE"
        self.clients: list[WebSocket] = []
        self.seen: set[str] = set()
        self.saved: set[str] = set()
        self._lock = asyncio.Lock()
        self._interval = 0.70
        self._next_start = 0.0
        self._cooldown_until = 0.0
        self._success_streak = 0
        self._recent_429 = 0


state = State()


def load_persistence():
    if AVAILABLE_FILE.exists():
        for line in AVAILABLE_FILE.read_text(encoding="utf-8").splitlines():
            n = line.strip().lower()
            if len(n) == 4:
                state.saved.add(n)
                state.seen.add(n)
    if SEEN_FILE.exists():
        for line in SEEN_FILE.read_text(encoding="utf-8").splitlines():
            n = line.strip().lower()
            if len(n) == 4:
                state.seen.add(n)
    state.available = len(state.saved)


def generate_leet_name() -> str:
    """Genera nombres de 4 caracteres estilo leet (letras + números) de forma totalmente aleatoria."""
    num_digits = random.randint(0, 4)
    num_letters = 4 - num_digits
    chars = []
    chars += [random.choice(LETTERS) for _ in range(num_letters)]
    chars += [random.choice(DIGITS) for _ in range(num_digits)]
    random.shuffle(chars)
    return "".join(chars)


def next_candidate() -> Optional[str]:
    for _ in range(500):
        name = generate_leet_name()
        if name not in state.seen and name not in state.saved:
            return name
    return None


async def wait_turn() -> bool:
    while state.running:
        now = time.monotonic()
        next_t = max(state._next_start, state._cooldown_until)
        wait = next_t - now
        if wait <= 0:
            state._next_start = now + state._interval
            return True
        await asyncio.sleep(min(max(wait, 0.02), 0.25))
    return False


def record_success():
    state._success_streak += 1
    if state._success_streak >= 30:
        state._interval = max(MIN_INTERVAL, state._interval - 0.02)
        state._success_streak = 0
        state._recent_429 = 0


def record_429(retry_after: float) -> bool:
    now = time.monotonic()
    state._success_streak = 0
    if now - getattr(state, "_last_429", 0) > 90:
        state._recent_429 = 0
    state._recent_429 += 1
    state._last_429 = now
    if now >= state._cooldown_until:
        state._interval = min(MAX_INTERVAL, max(0.95, state._interval * 1.7))
    cooldown = max(4.0, retry_after, state._interval * 4)
    state._cooldown_until = max(state._cooldown_until, now + cooldown)
    return state._recent_429 >= 3


async def validate(username: str, client: httpx.AsyncClient) -> tuple[str, str, float]:
    for attempt in range(2):
        if not state.running or not await wait_turn():
            return "SKIPPED", "cancelled", 0.0
        started = time.perf_counter()
        try:
            r = await client.get(
                API_URL,
                params={
                    "request.username": username,
                    "request.birthday": "2000-01-01",
                    "request.context": "Signup",
                },
                timeout=5.0,
            )
            elapsed = time.perf_counter() - started
            if r.status_code == 429:
                ra = float(r.headers.get("Retry-After", 8) or 8)
                if record_429(ra):
                    state.rate_limited = True
                    state.running = False
                    return "RATE LIMITED", "repeated 429", elapsed
                if attempt == 0:
                    continue
                return "RATE LIMITED", "429", elapsed
            if r.status_code in {500, 502, 503, 504} and attempt == 0:
                await asyncio.sleep(0.8)
                continue
            if r.status_code != 200:
                return "ERROR", f"HTTP {r.status_code}", elapsed
            data = r.json()
            code = data.get("code")
            record_success()
            if code == 0:
                return "AVAILABLE", "accepted", elapsed
            if code == 1:
                return "TAKEN", "in use", elapsed
            if code in (2, 10):
                return "INAPPROPRIATE", "rejected", elapsed
            if code == 3:
                return "INVALID", "not allowed", elapsed
            return "UNKNOWN", str(data.get("message", ""))[:80], elapsed
        except Exception as e:
            elapsed = time.perf_counter() - started
            if attempt == 0:
                await asyncio.sleep(0.4)
                continue
            return "ERROR", type(e).__name__, elapsed
    return "ERROR", "retries exhausted", 0.0


async def send_webhook(username: str, elapsed: float):
    if not WEBHOOK_URL:
        return
    payload = {
        "username": "KERNEL // 4C LEET",
        "allowed_mentions": {"parse": []},
        "embeds": [{
            "author": {"name": "KERNEL  /  LIVE 4C LEET"},
            "title": "✓  USERNAME AVAILABLE",
            "description": f"`{username}`   ·   `{elapsed*1000:.0f} ms`",
            "color": 0xFF0033,
            "footer": {"text": "LIVE ROBLOX VALIDATION  •  AVAILABILITY MAY CHANGE"},
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }],
    }
    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            await c.post(WEBHOOK_URL, json=payload)
    except Exception:
        pass


async def broadcast(msg: dict):
    dead = []
    for ws in state.clients:
        try:
            await ws.send_json(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        if ws in state.clients:
            state.clients.remove(ws)


async def worker(client: httpx.AsyncClient):
    while state.running:
        name = next_candidate()
        if name is None:
            continue
        state.seen.add(name)
        status, detail, elapsed = await validate(name, client)
        if status == "SKIPPED":
            continue
        state.checked += 1
        if status == "AVAILABLE":
            state.available += 1
            state.last_hit = name
            state.saved.add(name)
            async with state._lock:
                with AVAILABLE_FILE.open("a", encoding="utf-8") as f:
                    f.write(name + "\n")
            await send_webhook(name, elapsed)
            await broadcast({
                "type": "hit",
                "username": name,
                "elapsed_ms": round(elapsed * 1000),
                "available": state.available,
                "checked": state.checked,
            })
        elif status == "RATE LIMITED":
            state.last_status = "RATE LIMITED"
            await broadcast({"type": "status", "status": "RATE LIMITED"})
            break
        if state.checked % 25 == 0:
            await broadcast({
                "type": "progress",
                "checked": state.checked,
                "available": state.available,
                "interval": round(state._interval, 3),
                "status": state.last_status,
            })
        await asyncio.sleep(0)


async def hunter_loop():
    state.start_time = time.perf_counter()
    state.last_status = "HUNTING"
    async with httpx.AsyncClient(
        headers={"User-Agent": "Kernel4CWeb/1.0", "Accept": "application/json"},
        limits=httpx.Limits(max_connections=WORKERS + 2),
    ) as client:
        tasks = [asyncio.create_task(worker(client)) for _ in range(WORKERS)]
        await asyncio.gather(*tasks)
    state.running = False
    if state.last_status == "HUNTING":
        state.last_status = "STOPPED"
    await broadcast({"type": "status", "status": state.last_status})


@app.on_event("startup")
async def startup():
    load_persistence()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "available": state.available,
        "checked": state.checked,
        "running": state.running,
        "webhook": bool(WEBHOOK_URL),
    })


@app.get("/api/status")
async def api_status():
    uptime = int(time.perf_counter() - state.start_time) if state.start_time else 0
    return {
        "running": state.running,
        "checked": state.checked,
        "available": state.available,
        "last_hit": state.last_hit,
        "status": state.last_status,
        "interval": round(state._interval, 3),
        "uptime": uptime,
        "webhook_configured": bool(WEBHOOK_URL),
    }


@app.post("/api/start")
async def api_start():
    if state.running:
        return JSONResponse({"ok": False, "msg": "already running"}, 400)
    state.running = True
    state.rate_limited = False
    state.last_status = "HUNTING"
    asyncio.create_task(hunter_loop())
    return {"ok": True, "msg": "hunt started"}


@app.post("/api/stop")
async def api_stop():
    state.running = False
    state.last_status = "STOPPING"
    return {"ok": True}


@app.get("/api/hits")
async def api_hits():
    if not AVAILABLE_FILE.exists():
        return []
    return [l.strip() for l in AVAILABLE_FILE.read_text().splitlines() if l.strip()]


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    state.clients.append(ws)
    try:
        await ws.send_json({
            "type": "hello",
            "checked": state.checked,
            "available": state.available,
            "status": state.last_status,
            "running": state.running,
        })
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in state.clients:
            state.clients.remove(ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
