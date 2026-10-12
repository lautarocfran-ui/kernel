from __future__ import annotations

import asyncio
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
from fastapi.templating import Jinja2Templates

load_dotenv()

APP_NAME = "4L Hunter"
API_URL = "https://auth.roblox.com/v1/usernames/validate"
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()

WORKERS = 5
MIN_INTERVAL = 0.35
MAX_INTERVAL = 2.2

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
AVAILABLE_FILE = DATA_DIR / "available_4c.txt"
SEEN_FILE = DATA_DIR / "seen_4c.txt"
Path("templates").mkdir(exist_ok=True)

LETTERS = string.ascii_lowercase
DIGITS = string.digits

app = FastAPI(title=APP_NAME)
templates = Jinja2Templates(directory="templates")


class State:
    def __init__(self):
        self.running = False
        self.checked = 0
        self.available = 0
        self.start_time = 0.0
        self.last_hit: Optional[str] = None
        self.last_status = "IDLE"
        self.clients: list[WebSocket] = []
        self.seen: set[str] = set()
        self.saved: set[str] = set()
        self._lock = asyncio.Lock()
        self._interval = 0.42
        self._next_start = 0.0
        self._cooldown_until = 0.0
        self._success_streak = 0
        self._recent_429 = 0
        self._last_429 = 0.0
        self._seen_buffer: list[str] = []


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


async def flush_seen():
    if not state._seen_buffer:
        return
    async with state._lock:
        batch = state._seen_buffer[:]
        state._seen_buffer.clear()
        try:
            with SEEN_FILE.open("a", encoding="utf-8") as f:
                for name in batch:
                    f.write(name + "\n")
        except Exception:
            state._seen_buffer.extend(batch)


def generate_name() -> str:
    # mezcla realista de letras + números (estilo 4 letras usable)
    num_digits = random.randint(0, 3)
    chars = [random.choice(LETTERS) for _ in range(4 - num_digits)]
    chars += [random.choice(DIGITS) for _ in range(num_digits)]
    random.shuffle(chars)
    return "".join(chars)


def next_name() -> Optional[str]:
    for _ in range(800):
        name = generate_name()
        if name not in state.seen:
            return name
    return None


async def wait_turn() -> bool:
    while state.running:
        now = time.monotonic()
        wait = max(state._next_start, state._cooldown_until) - now
        if wait <= 0:
            state._next_start = now + state._interval
            return True
        await asyncio.sleep(min(max(wait, 0.012), 0.18))
    return False


def record_success():
    state._success_streak += 1
    if state._success_streak >= 18:
        state._interval = max(MIN_INTERVAL, state._interval - 0.03)
        state._success_streak = 0
        state._recent_429 = 0


def record_429(retry_after: float) -> bool:
    now = time.monotonic()
    state._success_streak = 0
    if now - state._last_429 > 70:
        state._recent_429 = 0
    state._recent_429 += 1
    state._last_429 = now
    if now >= state._cooldown_until:
        state._interval = min(MAX_INTERVAL, max(0.8, state._interval * 1.75))
    state._cooldown_until = max(state._cooldown_until, now + max(3.2, retry_after, state._interval * 3))
    return state._recent_429 >= 3


async def validate(username: str, client: httpx.AsyncClient) -> tuple[str, float]:
    for attempt in range(2):
        if not state.running or not await wait_turn():
            return "SKIPPED", 0.0
        t0 = time.perf_counter()
        try:
            r = await client.get(
                API_URL,
                params={
                    "request.username": username,
                    "request.birthday": "2000-01-01",
                    "request.context": "Signup",
                },
                timeout=4.2,
            )
            elapsed = time.perf_counter() - t0

            if r.status_code == 429:
                if record_429(float(r.headers.get("Retry-After", 6) or 6)):
                    state.running = False
                    return "RATE LIMITED", elapsed
                if attempt == 0:
                    continue
                return "RATE LIMITED", elapsed

            if r.status_code in {500, 502, 503, 504} and attempt == 0:
                await asyncio.sleep(0.45)
                continue

            if r.status_code != 200:
                return "ERROR", elapsed

            code = r.json().get("code")
            record_success()
            if code == 0:
                return "AVAILABLE", elapsed
            return "TAKEN", elapsed

        except Exception:
            if attempt == 0:
                await asyncio.sleep(0.25)
                continue
            return "ERROR", time.perf_counter() - t0
    return "ERROR", 0.0


async def send_webhook(username: str, elapsed: float) -> None:
    if not WEBHOOK_URL:
        return
    payload = {
        "username": "4L Hunter",
        "allowed_mentions": {"parse": []},
        "embeds": [{
            "author": {"name": "4L HUNTER  •  LIVE HIT"},
            "title": "✓  USERNAME AVAILABLE",
            "description": f"**`{username}`**\n\n`{elapsed*1000:.0f} ms`",
            "color": 0xFF0033,
            "footer": {"text": "4L Hunter"},
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }],
    }
    try:
        async with httpx.AsyncClient(timeout=7.0) as c:
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
        name = next_name()
        if not name:
            continue

        state.seen.add(name)
        state._seen_buffer.append(name)

        status, elapsed = await validate(name, client)
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

        if len(state._seen_buffer) >= 50:
            await flush_seen()

        if state.checked % 12 == 0:
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
    state.running = True
    async with httpx.AsyncClient(
        headers={"User-Agent": "4LHunter/3.0", "Accept": "application/json"},
        limits=httpx.Limits(max_connections=WORKERS + 2),
    ) as client:
        await asyncio.gather(*[asyncio.create_task(worker(client)) for _ in range(WORKERS)])
    await flush_seen()
    state.running = False
    if state.last_status == "HUNTING":
        state.last_status = "STOPPED"
    await broadcast({"type": "status", "status": state.last_status})


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>4L Hunter</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=JetBrains+Mono:wght@600;700&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{background:#000;color:#F0F0F5;font-family:'Space Grotesk',system-ui,sans-serif;min-height:100vh}
.wrap{max-width:860px;margin:0 auto;padding:28px 20px}
.rail{height:2px;background:linear-gradient(90deg,#FF0033,#FF2A4F,transparent);margin-bottom:24px}
header{display:flex;justify-content:space-between;align-items:center;margin-bottom:28px}
.brand{display:flex;align-items:center;gap:12px;font-weight:700;letter-spacing:1.4px;font-size:18px}
.logo{width:36px;height:36px;background:linear-gradient(145deg,#FF0033,#C40028);border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;color:#fff;box-shadow:0 0 24px rgba(255,0,51,.45)}
.state-box{display:flex;align-items:center;gap:8px;background:#0C0C10;border:1px solid #2A2A34;border-radius:20px;padding:7px 14px}
.dot{width:8px;height:8px;border-radius:50%;background:#FF0033}
.dot.on{background:#4CE0A0;box-shadow:0 0 0 0 rgba(76,224,160,.6);animation:p 1.3s infinite}
@keyframes p{0%{box-shadow:0 0 0 0 rgba(76,224,160,.6)}70%{box-shadow:0 0 0 10px transparent}}
.state{font-size:11px;font-weight:600;letter-spacing:1.3px;color:#FF0033}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:11px;margin-bottom:24px}
.card{background:#0A0A0E;border:1px solid #22222C;border-radius:12px;padding:16px 10px;text-align:center}
.card.green{border-color:#1A3328}
.num{font-size:23px;font-weight:700;font-family:'JetBrains Mono',monospace}
.card.green .num{color:#4CE0A0}
.lab{font-size:10px;color:#6B6B78;letter-spacing:1.4px;margin-top:4px}
.controls{display:flex;gap:10px;margin-bottom:26px;flex-wrap:wrap}
button{border:1px solid #2A2A34;background:#0C0C10;color:#E4E4EC;border-radius:9px;padding:11px 18px;font-weight:600;font-size:12px;cursor:pointer;font-family:'Space Grotesk',sans-serif}
button:disabled{opacity:.35;cursor:not-allowed}
button.primary{background:linear-gradient(135deg,#FF1748,#FF0033);border-color:#FF3158;color:#fff}
button.stop{color:#FF7086;border-color:#5A1528;background:#140308}
.badge{margin-left:auto;font-size:11px;font-weight:600;padding:6px 12px;border-radius:16px;background:#06110C;border:1px solid #2A5141;color:#68E5AD}
.badge.off{background:#110608;border-color:#4A2020;color:#FF7086}
.hits h2{font-size:11px;letter-spacing:1.5px;color:#6B6B78;margin-bottom:10px}
#hitlist{list-style:none;max-height:320px;overflow-y:auto;display:flex;flex-direction:column;gap:6px}
#hitlist li{display:flex;justify-content:space-between;padding:12px 14px;background:#08080C;border:1px solid #1C1C24;border-radius:9px;font-family:'JetBrains Mono',monospace}
.name{color:#4CE0A0;font-weight:700;font-size:14px}
.ms{color:#6B6B78;font-size:12px}
.empty{text-align:center;padding:36px;color:#4A4A55;font-size:13px}
.toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(70px);background:#0C0C10;border:1px solid #FF0033;padding:12px 20px;border-radius:10px;font-size:13px;opacity:0;transition:.35s;z-index:50}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
@media(max-width:600px){.stats{grid-template-columns:1fr 1fr}}
</style>
</head>
<body>
<div class="wrap">
  <div class="rail"></div>
  <header>
    <div class="brand"><div class="logo">4L</div>HUNTER</div>
    <div class="state-box"><div class="dot" id="dot"></div><div class="state" id="state">IDLE</div></div>
  </header>
  <section class="stats">
    <div class="card"><div class="num" id="checked">0</div><div class="lab">CHECKED</div></div>
    <div class="card green"><div class="num" id="available">0</div><div class="lab">AVAILABLE</div></div>
    <div class="card"><div class="num" id="interval">—</div><div class="lab">INTERVAL</div></div>
    <div class="card"><div class="num" id="uptime">00:00</div><div class="lab">UPTIME</div></div>
  </section>
  <section class="controls">
    <button id="start" class="primary">▶ START</button>
    <button id="stop" class="stop" disabled>■ STOP</button>
    <span class="badge off" id="badge">WEBHOOK OFF</span>
  </section>
  <section class="hits">
    <h2>LIVE HITS</h2>
    <ul id="hitlist"><li class="empty" id="empty">Waiting for available names…</li></ul>
  </section>
</div>
<div class="toast" id="toast"></div>
<script>
const $=id=>document.getElementById(id);
const stateEl=$('state'),dot=$('dot'),checkedEl=$('checked'),availableEl=$('available');
const intervalEl=$('interval'),uptimeEl=$('uptime'),startBtn=$('start'),stopBtn=$('stop');
const hitlist=$('hitlist'),empty=$('empty'),badge=$('badge'),toast=$('toast');
let startTs=0;
function showToast(m){toast.textContent=m;toast.classList.add('show');setTimeout(()=>toast.classList.remove('show'),2800)}
function fmt(s){return `${String(Math.floor(s/60)).padStart(2,'0')}:${String(s%60).padStart(2,'0')}`}
function setRunning(on){startBtn.disabled=on;stopBtn.disabled=!on;dot.classList.toggle('on',on);stateEl.style.color=on?'#4CE0A0':'#FF0033'}
startBtn.onclick=async()=>{if((await fetch('/api/start',{method:'POST'})).ok){setRunning(true);stateEl.textContent='HUNTING';startTs=Date.now();showToast('Hunt started')}};
stopBtn.onclick=async()=>{await fetch('/api/stop',{method:'POST'});setRunning(false);stateEl.textContent='STOPPED';showToast('Stopped')};
const ws=new WebSocket((location.protocol==='https:'?'wss:':'ws:')+'//'+location.host+'/ws');
ws.onmessage=e=>{const d=JSON.parse(e.data);
if(d.type==='hit'){if(empty)empty.remove();const li=document.createElement('li');li.innerHTML=`<span class="name">${d.username}</span><span class="ms">${d.elapsed_ms} ms</span>`;hitlist.prepend(li);availableEl.textContent=d.available.toLocaleString();checkedEl.textContent=d.checked.toLocaleString();showToast('Found: '+d.username)}
if(d.type==='progress'||d.type==='hello'){checkedEl.textContent=(d.checked||0).toLocaleString();availableEl.textContent=(d.available||0).toLocaleString();if(d.interval)intervalEl.textContent=d.interval+'s';if(d.status){stateEl.textContent=d.status;stateEl.style.color=d.status==='HUNTING'?'#4CE0A0':'#FF0033'}if(d.running!==undefined)setRunning(d.running)}
if(d.type==='status'){stateEl.textContent=d.status;stateEl.style.color=d.status==='HUNTING'?'#4CE0A0':'#F5BB58';if(d.status!=='HUNTING')setRunning(false)}};
setInterval(()=>{if(startTs)uptimeEl.textContent=fmt(Math.floor((Date.now()-startTs)/1000))},1000);
fetch('/api/status').then(r=>r.json()).then(d=>{checkedEl.textContent=d.checked.toLocaleString();availableEl.textContent=d.available.toLocaleString();stateEl.textContent=d.status;if(d.webhook_configured){badge.textContent='WEBHOOK ON';badge.classList.remove('off')}else{badge.textContent='WEBHOOK OFF';badge.classList.add('off')}if(d.running){setRunning(true);startTs=Date.now()-d.uptime*1000}});
</script>
</body>
</html>
"""


@app.on_event("startup")
async def startup():
    load_persistence()
    Path("templates/index.html").write_text(INDEX_HTML, encoding="utf-8")
    # Auto-start: empieza a buscar solo al levantar el servicio
    if not state.running:
        state.running = True
        state.last_status = "HUNTING"
        asyncio.create_task(hunter_loop())


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


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
        "seen": len(state.seen),
    }


@app.post("/api/start")
async def api_start():
    if state.running:
        return JSONResponse({"ok": False}, 400)
    state.running = True
    state.last_status = "HUNTING"
    asyncio.create_task(hunter_loop())
    return {"ok": True}


@app.post("/api/stop")
async def api_stop():
    state.running = False
    state.last_status = "STOPPING"
    await flush_seen()
    return {"ok": True}


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
