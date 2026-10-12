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
    num_digits = random.randint(0, 4)
    num_letters = 4 - num_digits
    chars = [random.choice(LETTERS) for _ in range(num_letters)]
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


async def send_webhook(username: str, elapsed: float, test: bool = False):
    if not WEBHOOK_URL:
        return False
    if test:
        payload = {
            "username": "4L Hunter",
            "allowed_mentions": {"parse": []},
            "embeds": [{
                "author": {"name": "4L HUNTER  •  SYSTEM"},
                "title": "⚡  Webhook Test Successful",
                "description": "Your Discord webhook is connected and working correctly.\n\nThe hunter is ready to notify you the moment a 4-letter username becomes available.",
                "color": 0xFF0033,
                "footer": {"text": "4L Hunter  •  Live Validation"},
                "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }],
        }
    else:
        payload = {
            "username": "4L Hunter",
            "allowed_mentions": {"parse": []},
            "embeds": [{
                "author": {"name": "4L HUNTER  •  LIVE HIT"},
                "title": "✓  USERNAME AVAILABLE",
                "description": f"**`{username}`**\n\nResponse time: `{elapsed*1000:.0f} ms`",
                "color": 0xFF0033,
                "footer": {"text": "4L Hunter  •  Availability may change"},
                "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }],
        }
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
        headers={"User-Agent": "4LHunter/1.0", "Accept": "application/json"},
        limits=httpx.Limits(max_connections=WORKERS + 2),
    ) as client:
        tasks = [asyncio.create_task(worker(client)) for _ in range(WORKERS)]
        await asyncio.gather(*tasks)
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
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800;900&family=JetBrains+Mono:wght@500;700&display=swap');

  * { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: #000;
    color: #F4F4F7;
    font-family: 'Inter', system-ui, sans-serif;
    min-height: 100vh;
    overflow-x: hidden;
  }

  #bg {
    position: fixed;
    inset: 0;
    z-index: 0;
    pointer-events: none;
  }

  .wrap {
    position: relative;
    z-index: 1;
    max-width: 920px;
    margin: 0 auto;
    padding: 24px 20px;
  }

  .rail {
    height: 3px;
    background: linear-gradient(90deg, #FF0033, #FF3355, #8A001C, transparent);
    border-radius: 2px;
    margin-bottom: 24px;
    animation: railPulse 3s ease-in-out infinite;
  }

  @keyframes railPulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
  }

  header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 28px;
  }

  .brand {
    display: flex;
    align-items: center;
    gap: 12px;
    font-weight: 900;
    letter-spacing: 2px;
    font-size: 18px;
  }

  .k {
    width: 36px;
    height: 36px;
    background: linear-gradient(135deg, #FF0033, #B00024);
    color: #fff;
    border-radius: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 18px;
    font-weight: 900;
    box-shadow: 0 0 28px rgba(255,0,51,0.55);
    animation: kGlow 2.6s ease-in-out infinite;
  }

  @keyframes kGlow {
    0%, 100% { box-shadow: 0 0 18px rgba(255,0,51,0.4); }
    50% { box-shadow: 0 0 36px rgba(255,0,51,0.75); }
  }

  .dim { color: #777985; font-weight: 500; letter-spacing: 1.5px; font-size: 14px; }

  .state-box {
    display: flex;
    align-items: center;
    gap: 9px;
    background: rgba(10,10,13,0.85);
    border: 1px solid #2A2A34;
    border-radius: 22px;
    padding: 7px 16px;
    backdrop-filter: blur(8px);
  }

  .pulse {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    background: #FF0033;
    transition: background 0.3s;
  }

  .pulse.on {
    background: #4CE0A0;
    animation: pulseAnim 1.3s infinite;
  }

  @keyframes pulseAnim {
    0% { box-shadow: 0 0 0 0 rgba(76,224,160,0.7); }
    70% { box-shadow: 0 0 0 12px rgba(76,224,160,0); }
    100% { box-shadow: 0 0 0 0 rgba(76,224,160,0); }
  }

  .state {
    font-size: 11px;
    font-weight: 800;
    letter-spacing: 1.4px;
    color: #FF0033;
    transition: color 0.3s;
  }

  .stats {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
    margin-bottom: 24px;
  }

  .card {
    background: rgba(8,8,11,0.8);
    border: 1px solid #25252D;
    border-radius: 14px;
    padding: 18px 12px;
    text-align: center;
    backdrop-filter: blur(6px);
    transition: border-color 0.25s, transform 0.25s, box-shadow 0.25s;
  }

  .card:hover {
    border-color: #FF0033;
    transform: translateY(-3px);
    box-shadow: 0 10px 30px rgba(255,0,51,0.15);
  }

  .card.green { border-color: #1A3A2A; }
  .card.green:hover {
    border-color: #4CE0A0;
    box-shadow: 0 10px 30px rgba(76,224,160,0.12);
  }

  .num {
    font-size: 26px;
    font-weight: 900;
    font-family: 'JetBrains Mono', monospace;
  }

  .card.green .num { color: #4CE0A0; }

  .lab {
    font-size: 10px;
    color: #777985;
    letter-spacing: 1.5px;
    margin-top: 6px;
    font-weight: 600;
  }

  .controls {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    align-items: center;
    margin-bottom: 28px;
  }

  button {
    border: 1px solid #2A2A34;
    background: rgba(10,10,13,0.9);
    color: #E0E0E8;
    border-radius: 10px;
    padding: 12px 22px;
    font-weight: 800;
    font-size: 12px;
    cursor: pointer;
    letter-spacing: 0.7px;
    transition: all 0.2s;
    font-family: 'Inter', sans-serif;
  }

  button:hover:not(:disabled) {
    border-color: #FF0033;
    background: #12050A;
  }

  button:disabled { opacity: 0.35; cursor: not-allowed; }

  button.primary {
    background: linear-gradient(135deg, #FF1748, #FF0033);
    border-color: #FF3158;
    color: #fff;
    box-shadow: 0 4px 24px rgba(255,0,51,0.4);
  }

  button.primary:hover:not(:disabled) {
    box-shadow: 0 6px 32px rgba(255,0,51,0.55);
    transform: translateY(-1px);
  }

  button.stop {
    color: #FF7086;
    border-color: #601225;
    background: #150308;
  }

  button.test {
    color: #8AB9FF;
    border-color: #2A3A55;
    background: #080C14;
  }

  button.test:hover:not(:disabled) {
    border-color: #4A7AFF;
    background: #0C1220;
  }

  .webhook {
    margin-left: auto;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.8px;
    padding: 7px 14px;
    border-radius: 20px;
    background: #06110C;
    border: 1px solid #2A5141;
    color: #68E5AD;
  }

  .webhook.off {
    background: #110608;
    border-color: #4A2020;
    color: #FF7086;
  }

  .hits-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
  }

  .hits h2 {
    font-size: 11px;
    letter-spacing: 1.6px;
    color: #858692;
    font-weight: 700;
  }

  .hits-count {
    font-size: 11px;
    color: #4CE0A0;
    font-weight: 700;
  }

  #hitlist {
    list-style: none;
    max-height: 360px;
    overflow-y: auto;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  #hitlist li {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 13px 16px;
    background: rgba(6,6,8,0.85);
    border: 1px solid #1B1B22;
    border-radius: 10px;
    font-family: 'JetBrains Mono', monospace;
    animation: slideIn 0.4s cubic-bezier(0.16, 1, 0.3, 1);
    backdrop-filter: blur(4px);
  }

  @keyframes slideIn {
    from { opacity: 0; transform: translateY(-12px) scale(0.96); }
    to { opacity: 1; transform: translateY(0) scale(1); }
  }

  .name {
    color: #4CE0A0;
    font-weight: 700;
    font-size: 15px;
    letter-spacing: 1.3px;
  }

  .ms { color: #777985; font-size: 12px; }

  .empty {
    text-align: center;
    padding: 48px 20px;
    color: #555;
    font-size: 13px;
  }

  .toast {
    position: fixed;
    bottom: 28px;
    left: 50%;
    transform: translateX(-50%) translateY(90px);
    background: #0A0A0D;
    border: 1px solid #FF0033;
    color: #F4F4F7;
    padding: 13px 24px;
    border-radius: 12px;
    font-size: 13px;
    font-weight: 600;
    opacity: 0;
    transition: all 0.4s cubic-bezier(0.16, 1, 0.3, 1);
    z-index: 100;
    pointer-events: none;
    box-shadow: 0 12px 40px rgba(255,0,51,0.3);
  }

  .toast.show {
    opacity: 1;
    transform: translateX(-50%) translateY(0);
  }

  @media (max-width: 640px) {
    .stats { grid-template-columns: 1fr 1fr; }
    .controls { flex-direction: column; align-items: stretch; }
    .webhook { margin-left: 0; text-align: center; }
  }
</style>
</head>
<body>
<canvas id="bg"></canvas>

<div class="wrap">
  <div class="rail"></div>

  <header>
    <div class="brand">
      <span class="k">4L</span>
      HUNTER
    </div>
    <div class="state-box">
      <div class="pulse" id="pulse"></div>
      <div class="state" id="state">IDLE</div>
    </div>
  </header>

  <section class="stats">
    <div class="card">
      <div class="num" id="checked">0</div>
      <div class="lab">CHECKED</div>
    </div>
    <div class="card green">
      <div class="num" id="available">0</div>
      <div class="lab">AVAILABLE</div>
    </div>
    <div class="card">
      <div class="num" id="interval">—</div>
      <div class="lab">INTERVAL</div>
    </div>
    <div class="card">
      <div class="num" id="uptime">00:00</div>
      <div class="lab">UPTIME</div>
    </div>
  </section>

  <section class="controls">
    <button id="start" class="primary">▶  START HUNT</button>
    <button id="stop" class="stop" disabled>■  STOP</button>
    <button id="test" class="test">⚡  TEST WEBHOOK</button>
    <span class="webhook off" id="webhook">WEBHOOK OFF</span>
  </section>

  <section class="hits">
    <div class="hits-header">
      <h2>LIVE HITS</h2>
      <span class="hits-count" id="hitsCount">0 hits</span>
    </div>
    <ul id="hitlist">
      <li class="empty" id="emptyMsg">No available usernames found yet…</li>
    </ul>
  </section>
</div>

<div class="toast" id="toast"></div>

<script>
/* ========== Interactive particle background ========== */
const canvas = document.getElementById('bg');
const ctx = canvas.getContext('2d');
let w, h, particles = [];
const mouse = { x: null, y: null, radius: 120 };

function resize() {
  w = canvas.width = window.innerWidth;
  h = canvas.height = window.innerHeight;
}
window.addEventListener('resize', resize);
resize();

window.addEventListener('mousemove', e => {
  mouse.x = e.clientX;
  mouse.y = e.clientY;
});
window.addEventListener('mouseleave', () => {
  mouse.x = null;
  mouse.y = null;
});

class Particle {
  constructor() {
    this.x = Math.random() * w;
    this.y = Math.random() * h;
    this.size = Math.random() * 2 + 0.6;
    this.baseX = this.x;
    this.baseY = this.y;
    this.density = Math.random() * 20 + 8;
    this.vx = (Math.random() - 0.5) * 0.3;
    this.vy = (Math.random() - 0.5) * 0.3;
  }
  draw() {
    ctx.beginPath();
    ctx.arc(this.x, this.y, this.size, 0, Math.PI * 2);
    ctx.fillStyle = 'rgba(255, 0, 51, 0.55)';
    ctx.fill();
  }
  update() {
    this.x += this.vx;
    this.y += this.vy;
    if (this.x < 0 || this.x > w) this.vx *= -1;
    if (this.y < 0 || this.y > h) this.vy *= -1;

    if (mouse.x !== null) {
      const dx = mouse.x - this.x;
      const dy = mouse.y - this.y;
      const dist = Math.sqrt(dx * dx + dy * dy);
      if (dist < mouse.radius) {
        const force = (mouse.radius - dist) / mouse.radius;
        this.x -= (dx / dist) * force * this.density * 0.4;
        this.y -= (dy / dist) * force * this.density * 0.4;
      }
    }
  }
}

function initParticles() {
  particles = [];
  const count = Math.min(90, Math.floor((w * h) / 14000));
  for (let i = 0; i < count; i++) particles.push(new Particle());
}
initParticles();
window.addEventListener('resize', initParticles);

function animate() {
  ctx.clearRect(0, 0, w, h);
  for (const p of particles) {
    p.update();
    p.draw();
  }
  // soft connections
  for (let i = 0; i < particles.length; i++) {
    for (let j = i + 1; j < particles.length; j++) {
      const dx = particles[i].x - particles[j].x;
      const dy = particles[i].y - particles[j].y;
      const dist = Math.sqrt(dx * dx + dy * dy);
      if (dist < 110) {
        ctx.beginPath();
        ctx.strokeStyle = `rgba(255, 0, 51, ${0.12 * (1 - dist / 110)})`;
        ctx.lineWidth = 0.6;
        ctx.moveTo(particles[i].x, particles[i].y);
        ctx.lineTo(particles[j].x, particles[j].y);
        ctx.stroke();
      }
    }
  }
  requestAnimationFrame(animate);
}
animate();

/* ========== App logic ========== */
const stateEl = document.getElementById('state');
const pulseEl = document.getElementById('pulse');
const checkedEl = document.getElementById('checked');
const availableEl = document.getElementById('available');
const intervalEl = document.getElementById('interval');
const uptimeEl = document.getElementById('uptime');
const startBtn = document.getElementById('start');
const stopBtn = document.getElementById('stop');
const testBtn = document.getElementById('test');
const hitlist = document.getElementById('hitlist');
const emptyMsg = document.getElementById('emptyMsg');
const hitsCount = document.getElementById('hitsCount');
const webhookEl = document.getElementById('webhook');
const toast = document.getElementById('toast');

let startTs = 0;
let hitTotal = 0;

function showToast(msg) {
  toast.textContent = msg;
  toast.classList.add('show');
  setTimeout(() => toast.classList.remove('show'), 3200);
}

function fmt(s) {
  const m = Math.floor(s / 60), sec = s % 60;
  return `${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}`;
}

function setRunning(on) {
  startBtn.disabled = on;
  stopBtn.disabled = !on;
  pulseEl.classList.toggle('on', on);
  stateEl.style.color = on ? '#4CE0A0' : '#FF0033';
}

startBtn.onclick = async () => {
  const r = await fetch('/api/start', { method: 'POST' });
  if (r.ok) {
    setRunning(true);
    stateEl.textContent = 'HUNTING';
    startTs = Date.now();
    showToast('Hunt started');
  }
};

stopBtn.onclick = async () => {
  await fetch('/api/stop', { method: 'POST' });
  setRunning(false);
  stateEl.textContent = 'STOPPED';
  showToast('Hunt stopped');
};

testBtn.onclick = async () => {
  testBtn.disabled = true;
  testBtn.textContent = 'SENDING…';
  try {
    const r = await fetch('/api/test-webhook', { method: 'POST' });
    const d = await r.json();
    if (d.ok) {
      showToast('✓ Test message sent to Discord');
    } else {
      showToast('✗ ' + (d.msg || 'Webhook failed'));
    }
  } catch {
    showToast('✗ Connection error');
  }
  testBtn.disabled = false;
  testBtn.textContent = '⚡  TEST WEBHOOK';
};

const ws = new WebSocket((location.protocol === 'https:' ? 'wss:' : 'ws:') + '//' + location.host + '/ws');

ws.onmessage = (e) => {
  const d = JSON.parse(e.data);

  if (d.type === 'hit') {
    if (emptyMsg) emptyMsg.remove();
    const li = document.createElement('li');
    li.innerHTML = `<span class="name">${d.username}</span><span class="ms">${d.elapsed_ms} ms</span>`;
    hitlist.prepend(li);
    availableEl.textContent = d.available.toLocaleString();
    checkedEl.textContent = d.checked.toLocaleString();
    hitTotal++;
    hitsCount.textContent = hitTotal + ' hits';
    showToast('Available: ' + d.username);
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
    webhookEl.textContent = 'WEBHOOK ON';
    webhookEl.classList.remove('off');
  } else {
    webhookEl.textContent = 'WEBHOOK OFF';
    webhookEl.classList.add('off');
  }
  if (d.running) {
    setRunning(true);
    startTs = Date.now() - (d.uptime * 1000);
  }
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
    state.rate_limited = False
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
    if ok:
        return {"ok": True, "msg": "Test sent"}
    return JSONResponse({"ok": False, "msg": "Failed to send"}, 500)


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
