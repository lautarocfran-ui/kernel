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
MIN_INTERVAL = 0.22
MAX_INTERVAL = 2.1

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
AVAILABLE_FILE = DATA_DIR / "available_4c.txt"
SEEN_FILE = DATA_DIR / "seen_4c.txt"
Path("templates").mkdir(exist_ok=True)

LETTERS = string.ascii_lowercase
DIGITS = string.digits
ALNUM = LETTERS + DIGITS

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
        self._interval = 0.28
        self._next_start = 0.0
        self._cooldown_until = 0.0
        self._success_streak = 0
        self._recent_429 = 0
        self._last_429 = 0.0
        self._seen_buffer: list[str] = []
        self.pool: list[str] = []
        self.pool_idx: int = 0


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


def build_high_value_pool() -> list[str]:
    """Espacio exhaustivo de TODOS los 4-char válidos que aún tienen chance.
    Tiers ordenados de mayor a menor probabilidad de estar libres.
    Cubre: 32_x, 0_2x, 0x_p, a3_b, 7k2m, 0042, etc.
    Excluye solo-letras (100 % tomados en 2026).
    """
    tiers: list[list[str]] = [[] for _ in range(6)]
    seen_local: set[str] = set()

    def add(tier: int, name: str):
        if name not in seen_local and len(name) == 4:
            # valid Roblox: at most one _, never first/last
            if name.count("_") > 1:
                return
            if name[0] == "_" or name[-1] == "_":
                return
            seen_local.add(name)
            tiers[tier].append(name)

    # ── Tier 0 (máxima chance): 2 dígitos + _ + alnum  ──  32_x, 7_0a, x_32
    for d1 in DIGITS:
        for d2 in DIGITS:
            for x in ALNUM:
                add(0, f"{d1}{d2}_{x}")   # DD_X   32_x
                add(0, f"{d1}_{d2}{x}")   # D_DX   3_2x
                add(0, f"{x}_{d1}{d2}")   # X_DD   x_32

    # ── Tier 1: 1 dígito al frente + _  ──  0_2x, 0x_p, 0_x2
    for d in DIGITS:
        for x in ALNUM:
            for y in ALNUM:
                add(1, f"{d}_{x}{y}")     # D_XX
                add(1, f"{d}{x}_{y}")     # DX_X

    # ── Tier 2: letra + dígito + _ + alnum  ──  a3_b, k0_x
    for x in LETTERS:
        for d in DIGITS:
            for y in ALNUM:
                add(2, f"{x}{d}_{y}")     # XD_X
                add(2, f"{x}_{d}{y}")     # X_DX

    # ── Tier 3: pure 4-digit  ──  0042, 1337, 9999
    for n in range(10000):
        add(3, f"{n:04d}")

    # ── Tier 4: 3 dígitos + 1 letra (cualquier orden) ──
    for d1 in DIGITS:
        for d2 in DIGITS:
            for d3 in DIGITS:
                for L in LETTERS:
                    chars = [d1, d2, d3, L]
                    # todas las posiciones de la letra
                    for i in range(4):
                        c = chars[:]
                        c[i], c[3] = c[3], c[i]  # swap letter into position i
                        add(4, "".join(c))

    # ── Tier 5: 2 dígitos + 2 letras (cualquier orden) ──
    for d1 in DIGITS:
        for d2 in DIGITS:
            for L1 in LETTERS:
                for L2 in LETTERS:
                    base = [d1, d2, L1, L2]
                    # un sample representativo de permutaciones (no las 24)
                    for _ in range(6):
                        random.shuffle(base)
                        add(5, "".join(base))

    # concatenar tiers en orden de prioridad y barajar solo dentro de cada tier
    pool: list[str] = []
    for t in tiers:
        random.shuffle(t)
        pool.extend(t)
    return pool


def next_name() -> Optional[str]:
    """Saca el siguiente candidato del pool exhaustivo (sin repetición).
    Cuando el pool se agota, regenera uno nuevo filtrando lo ya visto.
    """
    # avanzar por el pool pre-construido
    while state.pool_idx < len(state.pool):
        name = state.pool[state.pool_idx]
        state.pool_idx += 1
        if name not in state.seen:
            return name

    # pool agotado → reconstruir filtrando seen
    state.pool = [n for n in build_high_value_pool() if n not in state.seen]
    state.pool_idx = 0
    if not state.pool:
        return None
    name = state.pool[0]
    state.pool_idx = 1
    return name


async def wait_turn() -> bool:
    while state.running:
        now = time.monotonic()
        wait = max(state._next_start, state._cooldown_until) - now
        if wait <= 0:
            state._next_start = now + state._interval
            return True
        await asyncio.sleep(min(max(wait, 0.01), 0.16))
    return False


def record_success():
    state._success_streak += 1
    if state._success_streak >= 16:
        state._interval = max(MIN_INTERVAL, state._interval - 0.028)
        state._success_streak = 0
        state._recent_429 = 0


def record_429(retry_after: float) -> bool:
    now = time.monotonic()
    state._success_streak = 0
    if now - state._last_429 > 65:
        state._recent_429 = 0
    state._recent_429 += 1
    state._last_429 = now
    if now >= state._cooldown_until:
        state._interval = min(MAX_INTERVAL, max(0.75, state._interval * 1.8))
    state._cooldown_until = max(state._cooldown_until, now + max(3.0, retry_after, state._interval * 2.8))
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
                timeout=4.0,
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
                await asyncio.sleep(0.4)
                continue

            if r.status_code != 200:
                return "ERROR", elapsed

            code = r.json().get("code")
            record_success()
            return ("AVAILABLE", elapsed) if code == 0 else ("TAKEN", elapsed)

        except Exception:
            if attempt == 0:
                await asyncio.sleep(0.22)
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

        if len(state._seen_buffer) >= 40:
            await flush_seen()

        if state.checked % 10 == 0:
            await broadcast({
                "type": "progress",
                "checked": state.checked,
                "available": state.available,
                "interval": round(state._interval, 3),
                "status": state.last_status,
                "seen": len(state.seen),
            })
        await asyncio.sleep(0)


async def hunter_loop():
    state.start_time = time.perf_counter()
    state.last_status = "HUNTING"
    state.running = True
    async with httpx.AsyncClient(
        headers={"User-Agent": "4LHunter/4.0", "Accept": "application/json"},
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
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700&family=JetBrains+Mono:wght@500;700&display=swap');

*{box-sizing:border-box;margin:0;padding:0}
body{background:#000;color:#F2F2F7;font-family:'Outfit',system-ui,sans-serif;min-height:100vh;overflow-x:hidden}
#bg{position:fixed;inset:0;z-index:0;pointer-events:none}
.wrap{position:relative;z-index:1;max-width:880px;margin:0 auto;padding:32px 20px}

.rail{height:2px;background:linear-gradient(90deg,#FF0033,#FF2A4F 40%,transparent);margin-bottom:28px;border-radius:2px;opacity:.9}

header{display:flex;justify-content:space-between;align-items:center;margin-bottom:32px}
.brand{display:flex;align-items:center;gap:14px;font-weight:700;letter-spacing:1.6px;font-size:20px}
.logo{width:40px;height:40px;background:linear-gradient(145deg,#FF0033,#B00024);border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:700;color:#fff;box-shadow:0 0 28px rgba(255,0,51,.5)}
.state-box{display:flex;align-items:center;gap:10px;background:rgba(12,12,16,.8);border:1px solid #2A2A34;border-radius:24px;padding:8px 16px;backdrop-filter:blur(12px)}
.dot{width:8px;height:8px;border-radius:50%;background:#FF0033;transition:background .3s}
.dot.on{background:#4CE0A0;animation:pulse 1.4s infinite}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(76,224,160,.65)}70%{box-shadow:0 0 0 11px transparent}100%{box-shadow:0 0 0 0 transparent}}
.state{font-size:11px;font-weight:600;letter-spacing:1.5px;color:#FF0033}

.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:28px}
.card{background:rgba(10,10,14,.75);border:1px solid #22222C;border-radius:14px;padding:18px 12px;text-align:center;backdrop-filter:blur(8px);transition:border-color .25s,transform .25s}
.card:hover{border-color:#FF0033;transform:translateY(-2px)}
.card.green{border-color:#1A3328}
.card.green:hover{border-color:#4CE0A0}
.num{font-size:24px;font-weight:700;font-family:'JetBrains Mono',monospace}
.card.green .num{color:#4CE0A0}
.lab{font-size:10px;color:#6B6B78;letter-spacing:1.5px;margin-top:5px;font-weight:500}

.controls{display:flex;gap:10px;align-items:center;margin-bottom:28px;flex-wrap:wrap}
button{border:1px solid #2A2A34;background:rgba(12,12,16,.9);color:#E4E4EC;border-radius:10px;padding:11px 20px;font-weight:600;font-size:12px;cursor:pointer;letter-spacing:.5px;font-family:'Outfit',sans-serif;transition:all .2s}
button:hover:not(:disabled){border-color:#FF0033;background:#14050A}
button:disabled{opacity:.35;cursor:not-allowed}
button.stop{color:#FF7086;border-color:#5A1528;background:#140308}
.badge{margin-left:auto;font-size:11px;font-weight:600;letter-spacing:.7px;padding:7px 14px;border-radius:20px;background:#06110C;border:1px solid #2A5141;color:#68E5AD}
.badge.off{background:#110608;border-color:#4A2020;color:#FF7086}

.hits-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
.hits h2{font-size:11px;letter-spacing:1.8px;color:#6B6B78;font-weight:600}
.hits-count{font-size:11px;color:#4CE0A0;font-weight:600}

#hitlist{list-style:none;max-height:340px;overflow-y:auto;display:flex;flex-direction:column;gap:6px}
#hitlist li{display:flex;justify-content:space-between;align-items:center;padding:13px 16px;background:rgba(8,8,12,.85);border:1px solid #1C1C24;border-radius:10px;font-family:'JetBrains Mono',monospace;animation:in .35s cubic-bezier(.16,1,.3,1)}
@keyframes in{from{opacity:0;transform:translateY(-8px)}to{opacity:1;transform:none}}
.name{color:#4CE0A0;font-weight:700;font-size:14px;letter-spacing:1.1px}
.ms{color:#6B6B78;font-size:12px}
.empty{text-align:center;padding:42px 20px;color:#4A4A55;font-size:13px}

.toast{position:fixed;bottom:28px;left:50%;transform:translateX(-50%) translateY(80px);background:#0C0C10;border:1px solid #FF0033;color:#F2F2F7;padding:13px 22px;border-radius:12px;font-size:13px;font-weight:500;opacity:0;transition:all .4s cubic-bezier(.16,1,.3,1);z-index:50;pointer-events:none;box-shadow:0 12px 36px rgba(255,0,51,.25)}
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
    <div class="state-box"><div class="dot" id="dot"></div><div class="state" id="state">STARTING</div></div>
  </header>

  <section class="stats">
    <div class="card"><div class="num" id="checked">0</div><div class="lab">CHECKED</div></div>
    <div class="card green"><div class="num" id="available">0</div><div class="lab">AVAILABLE</div></div>
    <div class="card"><div class="num" id="interval">—</div><div class="lab">INTERVAL</div></div>
    <div class="card"><div class="num" id="uptime">00:00</div><div class="lab">UPTIME</div></div>
  </section>

  <section class="controls">
    <button id="stop" class="stop">■  STOP</button>
    <span class="badge off" id="badge">WEBHOOK OFF</span>
  </section>

  <section class="hits">
    <div class="hits-header">
      <h2>LIVE HITS</h2>
      <span class="hits-count" id="hitsCount">0 hits</span>
    </div>
    <ul id="hitlist"><li class="empty" id="empty">Waiting for available names…</li></ul>
  </section>
</div>
<div class="toast" id="toast"></div>

<script>
/* Interactive grid background */
const canvas = document.getElementById('bg');
const ctx = canvas.getContext('2d');
let w, h, nodes = [], mouse = {x: -9999, y: -9999};

function resize() {
  w = canvas.width = innerWidth;
  h = canvas.height = innerHeight;
  nodes = [];
  const gap = 52;
  const cols = Math.ceil(w / gap) + 1;
  const rows = Math.ceil(h / gap) + 1;
  for (let y = 0; y < rows; y++)
    for (let x = 0; x < cols; x++)
      nodes.push({ ox: x * gap, oy: y * gap, x: x * gap, y: y * gap, phase: Math.random() * Math.PI * 2 });
}
addEventListener('resize', resize);
addEventListener('mousemove', e => { mouse.x = e.clientX; mouse.y = e.clientY; });
addEventListener('mouseleave', () => { mouse.x = -9999; mouse.y = -9999; });

function draw(t) {
  ctx.clearRect(0, 0, w, h);
  const time = t * 0.001;
  for (const n of nodes) {
    n.x = n.ox + Math.sin(time * 0.65 + n.phase) * 5.5;
    n.y = n.oy + Math.cos(time * 0.5 + n.phase * 1.2) * 4.5;
    const dx = n.x - mouse.x, dy = n.y - mouse.y, dist = Math.sqrt(dx * dx + dy * dy);
    if (dist < 130) {
      const f = (130 - dist) / 130;
      n.x += (dx / dist) * f * 26;
      n.y += (dy / dist) * f * 26;
    }
  }
  for (let i = 0; i < nodes.length; i++) {
    const a = nodes[i];
    for (let j = i + 1; j < nodes.length; j++) {
      const b = nodes[j], dx = a.x - b.x, dy = a.y - b.y, d = Math.sqrt(dx * dx + dy * dy);
      if (d < 68) {
        ctx.beginPath();
        ctx.strokeStyle = `rgba(255,0,51,${0.13 * (1 - d / 68)})`;
        ctx.lineWidth = 0.65;
        ctx.moveTo(a.x, a.y);
        ctx.lineTo(b.x, b.y);
        ctx.stroke();
      }
    }
  }
  for (const n of nodes) {
    const dx = n.x - mouse.x, dy = n.y - mouse.y, dist = Math.sqrt(dx * dx + dy * dy), near = dist < 130;
    ctx.beginPath();
    ctx.arc(n.x, n.y, near ? 2.1 : 1.2, 0, Math.PI * 2);
    ctx.fillStyle = near ? 'rgba(255,40,80,0.9)' : 'rgba(255,0,51,0.32)';
    ctx.fill();
  }
  requestAnimationFrame(draw);
}
resize();
requestAnimationFrame(draw);

/* App */
const $ = id => document.getElementById(id);
const stateEl = $('state'), dot = $('dot'), checkedEl = $('checked'), availableEl = $('available');
const intervalEl = $('interval'), uptimeEl = $('uptime'), stopBtn = $('stop');
const hitlist = $('hitlist'), empty = $('empty'), hitsCount = $('hitsCount'), badge = $('badge'), toast = $('toast');
let startTs = 0, hits = 0;

function showToast(m) {
  toast.textContent = m;
  toast.classList.add('show');
  setTimeout(() => toast.classList.remove('show'), 2800);
}
function fmt(s) {
  return `${String(Math.floor(s / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;
}
function setRunning(on) {
  stopBtn.disabled = !on;
  dot.classList.toggle('on', on);
  stateEl.style.color = on ? '#4CE0A0' : '#FF0033';
}

stopBtn.onclick = async () => {
  await fetch('/api/stop', { method: 'POST' });
  setRunning(false);
  stateEl.textContent = 'STOPPED';
  showToast('Hunt stopped');
};

const ws = new WebSocket((location.protocol === 'https:' ? 'wss:' : 'ws:') + '//' + location.host + '/ws');
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
    showToast('Found: ' + d.username);
  }
  if (d.type === 'progress' || d.type === 'hello') {
    checkedEl.textContent = (d.checked || 0).toLocaleString();
    availableEl.textContent = (d.available || 0).toLocaleString();
    if (d.interval) intervalEl.textContent = d.interval + 's';
    if (d.status) {
      stateEl.textContent = d.status;
      stateEl.style.color = d.status === 'HUNTING' ? '#4CE0A0' : '#FF0033';
    }
    if (d.running !== undefined) setRunning(d.running);
  }
  if (d.type === 'status') {
    stateEl.textContent = d.status;
    stateEl.style.color = d.status === 'HUNTING' ? '#4CE0A0' : '#F5BB58';
    if (d.status !== 'HUNTING') setRunning(false);
  }
};

setInterval(() => {
  if (startTs) uptimeEl.textContent = fmt(Math.floor((Date.now() - startTs) / 1000));
}, 1000);

fetch('/api/status').then(r => r.json()).then(d => {
  checkedEl.textContent = d.checked.toLocaleString();
  availableEl.textContent = d.available.toLocaleString();
  stateEl.textContent = d.status;
  if (d.webhook_configured) {
    badge.textContent = 'WEBHOOK ON';
    badge.classList.remove('off');
  } else {
    badge.textContent = 'WEBHOOK OFF';
    badge.classList.add('off');
  }
  if (d.running) {
    setRunning(true);
    startTs = Date.now() - (d.uptime * 1000);
    stateEl.textContent = 'HUNTING';
  }
});
</script>
</body>
</html>
"""


@app.on_event("startup")
async def startup():
    load_persistence()
    # pool exhaustivo de los 4-char con más chance de estar libres
    state.pool = [n for n in build_high_value_pool() if n not in state.seen]
    state.pool_idx = 0
    Path("templates/index.html").write_text(INDEX_HTML, encoding="utf-8")
    # Auto-start on deploy
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
