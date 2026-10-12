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
WORKERS = max(1, min(6, int(os.getenv("WORKERS", "3"))))
MIN_INTERVAL = float(os.getenv("MIN_INTERVAL", "0.55"))
MAX_INTERVAL = float(os.getenv("MAX_INTERVAL", "2.8"))

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
        self._interval = 0.70
        self._next_start = 0.0
        self._cooldown_until = 0.0
        self._success_streak = 0
        self._recent_429 = 0
        self._last_429 = 0.0


state = State()


def load_persistence():
    for path, target in ((AVAILABLE_FILE, state.saved), (SEEN_FILE, state.seen)):
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                n = line.strip().lower()
                if len(n) == 4:
                    target.add(n)
                    state.seen.add(n)
    state.available = len(state.saved)


def generate_leet_name() -> str:
    num_digits = random.randint(0, 4)
    chars = [random.choice(LETTERS) for _ in range(4 - num_digits)]
    chars += [random.choice(DIGITS) for _ in range(num_digits)]
    random.shuffle(chars)
    return "".join(chars)


def next_candidate() -> Optional[str]:
    for _ in range(400):
        name = generate_leet_name()
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
        await asyncio.sleep(min(max(wait, 0.02), 0.25))
    return False


def record_success():
    state._success_streak += 1
    if state._success_streak >= 28:
        state._interval = max(MIN_INTERVAL, state._interval - 0.02)
        state._success_streak = 0
        state._recent_429 = 0


def record_429(retry_after: float) -> bool:
    now = time.monotonic()
    state._success_streak = 0
    if now - state._last_429 > 90:
        state._recent_429 = 0
    state._recent_429 += 1
    state._last_429 = now
    if now >= state._cooldown_until:
        state._interval = min(MAX_INTERVAL, max(0.95, state._interval * 1.65))
    state._cooldown_until = max(state._cooldown_until, now + max(4.0, retry_after, state._interval * 3.5))
    return state._recent_429 >= 3


async def validate(username: str, client: httpx.AsyncClient) -> tuple[str, str, float]:
    for attempt in range(2):
        if not state.running or not await wait_turn():
            return "SKIPPED", "cancelled", 0.0
        t0 = time.perf_counter()
        try:
            r = await client.get(
                API_URL,
                params={"request.username": username, "request.birthday": "2000-01-01", "request.context": "Signup"},
                timeout=5.0,
            )
            elapsed = time.perf_counter() - t0
            if r.status_code == 429:
                if record_429(float(r.headers.get("Retry-After", 8) or 8)):
                    state.running = False
                    return "RATE LIMITED", "repeated 429", elapsed
                if attempt == 0:
                    continue
                return "RATE LIMITED", "429", elapsed
            if r.status_code in {500, 502, 503, 504} and attempt == 0:
                await asyncio.sleep(0.7)
                continue
            if r.status_code != 200:
                return "ERROR", f"HTTP {r.status_code}", elapsed
            data = r.json()
            code = data.get("code")
            record_success()
            mapping = {0: ("AVAILABLE", "accepted"), 1: ("TAKEN", "in use"), 2: ("INAPPROPRIATE", "rejected"),
                       3: ("INVALID", "not allowed"), 10: ("INAPPROPRIATE", "rejected")}
            return mapping.get(code, ("UNKNOWN", str(data.get("message", ""))[:80])) + (elapsed,)
        except Exception as e:
            elapsed = time.perf_counter() - t0
            if attempt == 0:
                await asyncio.sleep(0.35)
                continue
            return "ERROR", type(e).__name__, elapsed
    return "ERROR", "retries exhausted", 0.0


async def send_webhook(username: str, elapsed: float, test: bool = False) -> bool:
    if not WEBHOOK_URL:
        return False
    if test:
        embed = {
            "author": {"name": "4L HUNTER  •  SYSTEM"},
            "title": "⚡  Webhook Test Successful",
            "description": "Your Discord webhook is connected and working.\nYou will be notified the moment a 4-letter username becomes available.",
            "color": 0xFF0033,
            "footer": {"text": "4L Hunter"},
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
    else:
        embed = {
            "author": {"name": "4L HUNTER  •  LIVE HIT"},
            "title": "✓  USERNAME AVAILABLE",
            "description": f"**`{username}`**\n\nResponse time · `{elapsed*1000:.0f} ms`",
            "color": 0xFF0033,
            "footer": {"text": "4L Hunter  •  Availability may change"},
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
    payload = {"username": "4L Hunter", "allowed_mentions": {"parse": []}, "embeds": [embed]}
    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.post(WEBHOOK_URL, json=payload)
            return r.status_code in (200, 204)
    except Exception:
        return False


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
        if not name:
            continue
        state.seen.add(name)
        status, _, elapsed = await validate(name, client)
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
            await broadcast({"type": "hit", "username": name, "elapsed_ms": round(elapsed * 1000),
                             "available": state.available, "checked": state.checked})
        elif status == "RATE LIMITED":
            state.last_status = "RATE LIMITED"
            await broadcast({"type": "status", "status": "RATE LIMITED"})
            break
        if state.checked % 20 == 0:
            await broadcast({"type": "progress", "checked": state.checked, "available": state.available,
                             "interval": round(state._interval, 3), "status": state.last_status})
        await asyncio.sleep(0)


async def hunter_loop():
    state.start_time = time.perf_counter()
    state.last_status = "HUNTING"
    async with httpx.AsyncClient(
        headers={"User-Agent": "4LHunter/2.0", "Accept": "application/json"},
        limits=httpx.Limits(max_connections=WORKERS + 2),
    ) as client:
        await asyncio.gather(*[asyncio.create_task(worker(client)) for _ in range(WORKERS)])
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
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@500;700&display=swap');

*{box-sizing:border-box;margin:0;padding:0}
body{background:#000;color:#F0F0F5;font-family:'Space Grotesk',system-ui,sans-serif;min-height:100vh;overflow-x:hidden}
#bg{position:fixed;inset:0;z-index:0;pointer-events:none}
.wrap{position:relative;z-index:1;max-width:900px;margin:0 auto;padding:28px 20px}

.rail{height:2px;background:linear-gradient(90deg,#FF0033,#FF2A4F,transparent);margin-bottom:26px;border-radius:2px;opacity:.85}

header{display:flex;justify-content:space-between;align-items:center;margin-bottom:30px}
.brand{display:flex;align-items:center;gap:12px;font-weight:700;letter-spacing:1.5px;font-size:19px}
.logo{width:38px;height:38px;background:linear-gradient(145deg,#FF0033,#C40028);border-radius:11px;display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:700;color:#fff;box-shadow:0 0 30px rgba(255,0,51,.5);animation:glow 3s ease-in-out infinite}
@keyframes glow{0%,100%{box-shadow:0 0 18px rgba(255,0,51,.4)}50%{box-shadow:0 0 36px rgba(255,0,51,.7)}}
.sub{color:#6B6B78;font-weight:500;font-size:13px;letter-spacing:2px}

.state-box{display:flex;align-items:center;gap:9px;background:rgba(12,12,16,.75);border:1px solid #2A2A34;border-radius:24px;padding:8px 16px;backdrop-filter:blur(10px)}
.dot{width:8px;height:8px;border-radius:50%;background:#FF0033;transition:background .3s}
.dot.on{background:#4CE0A0;animation:pulse 1.4s infinite}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(76,224,160,.7)}70%{box-shadow:0 0 0 11px transparent}100%{box-shadow:0 0 0 0 transparent}}
.state{font-size:11px;font-weight:600;letter-spacing:1.5px;color:#FF0033}

.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:26px}
.card{background:rgba(10,10,14,.7);border:1px solid #22222C;border-radius:14px;padding:18px 10px;text-align:center;backdrop-filter:blur(8px);transition:border-color .25s,transform .25s}
.card:hover{border-color:#FF0033;transform:translateY(-2px)}
.card.green{border-color:#1A3328}
.card.green:hover{border-color:#4CE0A0}
.num{font-size:25px;font-weight:700;font-family:'JetBrains Mono',monospace}
.card.green .num{color:#4CE0A0}
.lab{font-size:10px;color:#6B6B78;letter-spacing:1.6px;margin-top:5px;font-weight:500}

.controls{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-bottom:30px}
button{border:1px solid #2A2A34;background:rgba(12,12,16,.85);color:#E4E4EC;border-radius:10px;padding:12px 20px;font-weight:600;font-size:12px;cursor:pointer;letter-spacing:.6px;font-family:'Space Grotesk',sans-serif;transition:all .2s}
button:hover:not(:disabled){border-color:#FF0033;background:#14050A}
button:disabled{opacity:.35;cursor:not-allowed}
button.primary{background:linear-gradient(135deg,#FF1748,#FF0033);border-color:#FF3158;color:#fff;box-shadow:0 4px 22px rgba(255,0,51,.35)}
button.primary:hover:not(:disabled){box-shadow:0 6px 28px rgba(255,0,51,.5);transform:translateY(-1px)}
button.stop{color:#FF7086;border-color:#5A1528;background:#140308}
button.test{color:#8AB9FF;border-color:#2A3A55;background:#080C14}
button.test:hover:not(:disabled){border-color:#4A7AFF}

.badge{margin-left:auto;font-size:11px;font-weight:600;letter-spacing:.8px;padding:7px 14px;border-radius:20px;background:#06110C;border:1px solid #2A5141;color:#68E5AD}
.badge.off{background:#110608;border-color:#4A2020;color:#FF7086}

.hits-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
.hits h2{font-size:11px;letter-spacing:1.8px;color:#6B6B78;font-weight:600}
.hits-count{font-size:11px;color:#4CE0A0;font-weight:600}

#hitlist{list-style:none;max-height:340px;overflow-y:auto;display:flex;flex-direction:column;gap:6px}
#hitlist li{display:flex;justify-content:space-between;align-items:center;padding:13px 16px;background:rgba(8,8,12,.8);border:1px solid #1C1C24;border-radius:10px;font-family:'JetBrains Mono',monospace;animation:in .4s cubic-bezier(.16,1,.3,1);backdrop-filter:blur(6px)}
@keyframes in{from{opacity:0;transform:translateY(-10px)}to{opacity:1;transform:none}}
.name{color:#4CE0A0;font-weight:700;font-size:14px;letter-spacing:1.2px}
.ms{color:#6B6B78;font-size:12px}
.empty{text-align:center;padding:44px 20px;color:#4A4A55;font-size:13px}

.toast{position:fixed;bottom:28px;left:50%;transform:translateX(-50%) translateY(80px);background:#0C0C10;border:1px solid #FF0033;color:#F0F0F5;padding:13px 24px;border-radius:12px;font-size:13px;font-weight:500;opacity:0;transition:all .4s cubic-bezier(.16,1,.3,1);z-index:50;pointer-events:none;box-shadow:0 12px 40px rgba(255,0,51,.25)}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}

@media(max-width:640px){.stats{grid-template-columns:1fr 1fr}.controls{flex-direction:column;align-items:stretch}.badge{margin-left:0;text-align:center}}
</style>
</head>
<body>
<canvas id="bg"></canvas>
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
    <button id="start" class="primary">▶  START HUNT</button>
    <button id="stop" class="stop" disabled>■  STOP</button>
    <button id="test" class="test">⚡  TEST WEBHOOK</button>
    <span class="badge off" id="badge">WEBHOOK OFF</span>
  </section>

  <section class="hits">
    <div class="hits-header"><h2>LIVE HITS</h2><span class="hits-count" id="hitsCount">0 hits</span></div>
    <ul id="hitlist"><li class="empty" id="empty">No available usernames found yet…</li></ul>
  </section>
</div>
<div class="toast" id="toast"></div>

<script>
/* ---- Interactive flowing grid background ---- */
const canvas = document.getElementById('bg');
const ctx = canvas.getContext('2d');
let w, h, nodes = [], mouse = {x: -9999, y: -9999};

function resize() {
  w = canvas.width = window.innerWidth;
  h = canvas.height = window.innerHeight;
  init();
}
window.addEventListener('resize', resize);

window.addEventListener('mousemove', e => { mouse.x = e.clientX; mouse.y = e.clientY; });
window.addEventListener('mouseleave', () => { mouse.x = -9999; mouse.y = -9999; });

function init() {
  nodes = [];
  const gap = 48;
  const cols = Math.ceil(w / gap) + 1;
  const rows = Math.ceil(h / gap) + 1;
  for (let y = 0; y < rows; y++) {
    for (let x = 0; x < cols; x++) {
      nodes.push({
        ox: x * gap,
        oy: y * gap,
        x: x * gap,
        y: y * gap,
        phase: Math.random() * Math.PI * 2,
      });
    }
  }
}

function draw(t) {
  ctx.clearRect(0, 0, w, h);
  const time = t * 0.001;

  for (const n of nodes) {
    // gentle wave motion
    n.x = n.ox + Math.sin(time * 0.7 + n.phase) * 6;
    n.y = n.oy + Math.cos(time * 0.55 + n.phase * 1.3) * 5;

    // mouse repulsion
    const dx = n.x - mouse.x;
    const dy = n.y - mouse.y;
    const dist = Math.sqrt(dx * dx + dy * dy);
    if (dist < 140) {
      const f = (140 - dist) / 140;
      n.x += (dx / dist) * f * 28;
      n.y += (dy / dist) * f * 28;
    }
  }

  // connections
  for (let i = 0; i < nodes.length; i++) {
    const a = nodes[i];
    for (let j = i + 1; j < nodes.length; j++) {
      const b = nodes[j];
      const dx = a.x - b.x;
      const dy = a.y - b.y;
      const d = Math.sqrt(dx * dx + dy * dy);
      if (d < 70) {
        ctx.beginPath();
        ctx.strokeStyle = `rgba(255,0,51,${0.14 * (1 - d / 70)})`;
        ctx.lineWidth = 0.7;
        ctx.moveTo(a.x, a.y);
        ctx.lineTo(b.x, b.y);
        ctx.stroke();
      }
    }
  }

  // nodes
  for (const n of nodes) {
    const dx = n.x - mouse.x;
    const dy = n.y - mouse.y;
    const dist = Math.sqrt(dx * dx + dy * dy);
    const near = dist < 140;
    ctx.beginPath();
    ctx.arc(n.x, n.y, near ? 2.2 : 1.3, 0, Math.PI * 2);
    ctx.fillStyle = near ? 'rgba(255,40,80,0.9)' : 'rgba(255,0,51,0.35)';
    ctx.fill();
  }

  requestAnimationFrame(draw);
}

resize();
requestAnimationFrame(draw);

/* ---- App ---- */
const $ = id => document.getElementById(id);
const stateEl = $('state'), dot = $('dot'), checkedEl = $('checked'), availableEl = $('available');
const intervalEl = $('interval'), uptimeEl = $('uptime'), startBtn = $('start'), stopBtn = $('stop');
const testBtn = $('test'), hitlist = $('hitlist'), empty = $('empty'), hitsCount = $('hitsCount');
const badge = $('badge'), toast = $('toast');

let startTs = 0, hits = 0;

function showToast(msg) {
  toast.textContent = msg;
  toast.classList.add('show');
  setTimeout(() => toast.classList.remove('show'), 3000);
}

function fmt(s) {
  return `${String(Math.floor(s/60)).padStart(2,'0')}:${String(s%60).padStart(2,'0')}`;
}

function setRunning(on) {
  startBtn.disabled = on;
  stopBtn.disabled = !on;
  dot.classList.toggle('on', on);
  stateEl.style.color = on ? '#4CE0A0' : '#FF0033';
}

startBtn.onclick = async () => {
  if ((await fetch('/api/start', {method:'POST'})).ok) {
    setRunning(true);
    stateEl.textContent = 'HUNTING';
    startTs = Date.now();
    showToast('Hunt started');
  }
};

stopBtn.onclick = async () => {
  await fetch('/api/stop', {method:'POST'});
  setRunning(false);
  stateEl.textContent = 'STOPPED';
  showToast('Hunt stopped');
};

testBtn.onclick = async () => {
  testBtn.disabled = true;
  testBtn.textContent = 'SENDING…';
  try {
    const r = await fetch('/api/test-webhook', {method:'POST'});
    const d = await r.json();
    showToast(d.ok ? '✓ Test sent to Discord' : '✗ ' + (d.msg || 'Failed'));
  } catch { showToast('✗ Connection error'); }
  testBtn.disabled = false;
  testBtn.textContent = '⚡  TEST WEBHOOK';
};

const ws = new WebSocket((location.protocol==='https:'?'wss:':'ws:')+'//'+location.host+'/ws');
ws.onmessage = e => {
  const d = JSON.parse(e.data);
  if (d.type === 'hit') {
    if (empty) empty.remove();
    const li = document.createElement('li');
    li.innerHTML = `<span class="name">${d.username}</span><span class="ms">${d.elapsed_ms} ms</span>`;
    hitlist.prepend(li);
    availableEl.textContent = d.available.toLocaleString();
    checkedEl.textContent = d.checked.toLocaleString();
    hits++;
    hitsCount.textContent = hits + ' hits';
    showToast('Available: ' + d.username);
  }
  if (d.type === 'progress' || d.type === 'hello') {
    checkedEl.textContent = (d.checked||0).toLocaleString();
    availableEl.textContent = (d.available||0).toLocaleString();
    if (d.interval) intervalEl.textContent = d.interval + 's';
    if (d.status) { stateEl.textContent = d.status; stateEl.style.color = d.status==='HUNTING'?'#4CE0A0':'#FF0033'; }
    if (d.running !== undefined) setRunning(d.running);
  }
  if (d.type === 'status') {
    stateEl.textContent = d.status;
    stateEl.style.color = d.status==='HUNTING'?'#4CE0A0':'#F5BB58';
    if (d.status !== 'HUNTING') setRunning(false);
  }
};

setInterval(() => { if (startTs) uptimeEl.textContent = fmt(Math.floor((Date.now()-startTs)/1000)); }, 1000);

fetch('/api/status').then(r=>r.json()).then(d => {
  checkedEl.textContent = d.checked.toLocaleString();
  availableEl.textContent = d.available.toLocaleString();
  stateEl.textContent = d.status;
  if (d.webhook_configured) { badge.textContent = 'WEBHOOK ON'; badge.classList.remove('off'); }
  else { badge.textContent = 'WEBHOOK OFF'; badge.classList.add('off'); }
  if (d.running) { setRunning(true); startTs = Date.now() - d.uptime*1000; }
});
</script>
</body>
</html>
"""


@app.on_event("startup")
async def startup():
    load_persistence()
    Path("templates/index.html").write_text(INDEX_HTML, encoding="utf-8")


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
    state.last_status = "HUNTING"
    asyncio.create_task(hunter_loop())
    return {"ok": True}


@app.post("/api/stop")
async def api_stop():
    state.running = False
    state.last_status = "STOPPING"
    return {"ok": True}


@app.post("/api/test-webhook")
async def api_test_webhook():
    if not WEBHOOK_URL:
        return JSONResponse({"ok": False, "msg": "No webhook configured"}, 400)
    ok = await send_webhook("test", 0.0, test=True)
    return {"ok": ok, "msg": "Test sent" if ok else "Failed to send"}


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
