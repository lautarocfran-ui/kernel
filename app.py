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
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

load_dotenv()

APP_NAME = "Kernel"
API_URL = "https://auth.roblox.com/v1/usernames/validate"
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()

WORKERS = 5
MIN_INTERVAL = 0.28
MAX_INTERVAL = 2.0

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
AVAILABLE_FILE = DATA_DIR / "available_5c.txt"
SEEN_FILE = DATA_DIR / "seen_5c.txt"
Path("templates").mkdir(exist_ok=True)
Path("static").mkdir(exist_ok=True)

LETTERS = string.ascii_lowercase
DIGITS = string.digits

app = FastAPI(title=APP_NAME)
try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except Exception:
    pass
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
        self._interval = 0.36
        self._next_start = 0.0
        self._cooldown_until = 0.0
        self._success_streak = 0
        self._recent_429 = 0
        self._last_429 = 0.0
        self._seen_buffer: list[str] = []


state = State()


def load_persistence():
    for path, target in ((AVAILABLE_FILE, state.saved), (SEEN_FILE, state.seen)):
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            n = line.strip().lower()
            if len(n) == 5:
                target.add(n)
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


def random_5() -> str:
    """5 chars fully random: mostly letters + 1 number in a random position (a9wen, k3lmp…)."""
    chars = [random.choice(LETTERS) for _ in range(5)]
    # put 1 digit in a random slot (not forced pattern)
    pos = random.randint(0, 4)
    chars[pos] = random.choice(DIGITS)
    # ~25% chance of a second digit somewhere else
    if random.random() < 0.25:
        pos2 = random.randint(0, 4)
        if pos2 != pos:
            chars[pos2] = random.choice(DIGITS)
    return "".join(chars)


def next_name() -> Optional[str]:
    for _ in range(1500):
        name = random_5()
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
        await asyncio.sleep(min(max(wait, 0.01), 0.15))
    return False


def record_success():
    state._success_streak += 1
    if state._success_streak >= 18:
        state._interval = max(MIN_INTERVAL, state._interval - 0.025)
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
        state._interval = min(MAX_INTERVAL, max(0.72, state._interval * 1.75))
    state._cooldown_until = max(
        state._cooldown_until, now + max(3.0, retry_after, state._interval * 2.8)
    )
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
                await asyncio.sleep(0.2)
                continue
            return "ERROR", time.perf_counter() - t0
    return "ERROR", 0.0


async def send_webhook(username: str, elapsed: float) -> None:
    if not WEBHOOK_URL:
        return
    payload = {
        "username": "Kernel",
        "allowed_mentions": {"parse": []},
        "embeds": [{
            "author": {"name": "KERNEL  •  LIVE HIT"},
            "title": "✓  USERNAME AVAILABLE",
            "description": f"**`{username}`**\n\n`{elapsed*1000:.0f} ms`",
            "color": 0xFF0033,
            "footer": {"text": "Kernel"},
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
        headers={"User-Agent": "Kernel/2.1", "Accept": "application/json"},
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
<title>Kernel</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect width='100' height='100' rx='22' fill='%23FF0033'/><text x='50' y='68' font-size='52' font-weight='700' fill='white' text-anchor='middle' font-family='system-ui'>K</text></svg>">
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700&family=JetBrains+Mono:wght@500;700&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{background:#000;color:#F2F2F7;font-family:'Outfit',system-ui,sans-serif;min-height:100vh;overflow-x:hidden}
#bg{position:fixed;inset:0;z-index:0;pointer-events:none}
.wrap{position:relative;z-index:1;max-width:920px;margin:0 auto;padding:28px 18px}
.rail{height:2px;background:linear-gradient(90deg,#FF0033,#FF2A4F 50%,transparent);margin-bottom:24px;border-radius:2px;opacity:.95}
header{display:flex;justify-content:space-between;align-items:center;margin-bottom:26px;gap:12px;flex-wrap:wrap}
.brand{display:flex;align-items:center;gap:12px;font-weight:700;letter-spacing:2.2px;font-size:20px}
.logo{width:42px;height:42px;border-radius:12px;overflow:hidden;box-shadow:0 0 28px rgba(255,0,51,.5);background:linear-gradient(145deg,#FF0033,#B00024);display:flex;align-items:center;justify-content:center;transition:transform .25s,box-shadow .25s}
.logo:hover{transform:scale(1.06);box-shadow:0 0 36px rgba(255,0,51,.7)}
.logo img{width:100%;height:100%;object-fit:cover}
.logo-fallback{color:#fff;font-weight:700;font-size:18px}
.state-box{display:flex;align-items:center;gap:9px;background:rgba(12,12,16,.88);border:1px solid #2A2A34;border-radius:24px;padding:8px 15px;backdrop-filter:blur(12px);transition:border-color .25s}
.state-box:hover{border-color:#FF0033}
.dot{width:8px;height:8px;border-radius:50%;background:#FF0033}
.dot.on{background:#4CE0A0;animation:pulse 1.35s infinite}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(76,224,160,.65)}70%{box-shadow:0 0 0 11px transparent}}
.state{font-size:11px;font-weight:600;letter-spacing:1.5px;color:#FF0033}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:22px}
.card{background:rgba(10,10,14,.8);border:1px solid #22222C;border-radius:14px;padding:18px 10px;text-align:center;backdrop-filter:blur(10px);transition:border-color .25s,transform .25s,box-shadow .25s;cursor:default}
.card:hover{border-color:#FF0033;transform:translateY(-3px);box-shadow:0 10px 28px rgba(255,0,51,.12)}
.card.green{border-color:#1A3328}
.card.green:hover{border-color:#4CE0A0;box-shadow:0 10px 28px rgba(76,224,160,.1)}
.num{font-size:23px;font-weight:700;font-family:'JetBrains Mono',monospace}
.card.green .num{color:#4CE0A0}
.lab{font-size:10px;color:#6B6B78;letter-spacing:1.5px;margin-top:5px;font-weight:500}
.controls{display:flex;gap:8px;align-items:center;margin-bottom:20px;flex-wrap:wrap}
button,select{border:1px solid #2A2A34;background:rgba(12,12,16,.92);color:#E4E4EC;border-radius:10px;padding:10px 16px;font-weight:600;font-size:12px;cursor:pointer;font-family:'Outfit',sans-serif;transition:all .2s}
button:hover:not(:disabled),select:hover{border-color:#FF0033;background:#14050A}
button:disabled{opacity:.35;cursor:not-allowed}
button.stop{color:#FF7086;border-color:#5A1528;background:#140308}
button.clear{color:#8AB9FF;border-color:#2A3A55;background:#080C14}
select{min-width:148px;appearance:none}
.badge{margin-left:auto;font-size:11px;font-weight:600;padding:7px 13px;border-radius:18px;background:#06110C;border:1px solid #2A5141;color:#68E5AD;transition:all .2s}
.badge.off{background:#110608;border-color:#4A2020;color:#FF7086}
.hits-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
.hits h2{font-size:11px;letter-spacing:1.7px;color:#6B6B78;font-weight:600}
.hits-count{font-size:11px;color:#4CE0A0;font-weight:600}
#hitlist{list-style:none;max-height:360px;overflow-y:auto;display:flex;flex-direction:column;gap:6px}
#hitlist li{display:flex;justify-content:space-between;align-items:center;padding:13px 15px;background:rgba(8,8,12,.9);border:1px solid #1C1C24;border-radius:11px;font-family:'JetBrains Mono',monospace;animation:in .32s ease;transition:border-color .2s,transform .2s}
#hitlist li:hover{border-color:#FF0033;transform:translateX(3px)}
@keyframes in{from{opacity:0;transform:translateY(-8px)}to{opacity:1;transform:none}}
.name{color:#4CE0A0;font-weight:700;font-size:14px;letter-spacing:1.2px}
.ms{color:#6B6B78;font-size:12px}
.empty{text-align:center;padding:44px 16px;color:#4A4A55;font-size:13px}
.toast{position:fixed;bottom:26px;left:50%;transform:translateX(-50%) translateY(80px);background:#0C0C10;border:1px solid #FF0033;padding:13px 22px;border-radius:12px;font-size:13px;opacity:0;transition:.4s cubic-bezier(.16,1,.3,1);z-index:50;pointer-events:none;box-shadow:0 12px 36px rgba(255,0,51,.28)}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
@media(max-width:640px){.stats{grid-template-columns:1fr 1fr}.badge{margin-left:0}}
</style>
</head>
<body>
<canvas id="bg"></canvas>
<div class="wrap">
  <div class="rail"></div>
  <header>
    <div class="brand">
      <div class="logo">
        <img src="/static/icon.png" alt="" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'">
        <span class="logo-fallback" style="display:none">K</span>
      </div>
      KERNEL
    </div>
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
    <button id="clear" class="clear">CLEAR LIST</button>
    <select id="bgSelect" title="Background">
      <option value="0">Grid Flow</option>
      <option value="1">Particles</option>
      <option value="2">Aurora</option>
      <option value="3">Hex Field</option>
      <option value="4">Star Drift</option>
      <option value="5">Ripple Orbs</option>
    </select>
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
const canvas=document.getElementById('bg'),ctx=canvas.getContext('2d');
let w,h,mode=0,mouse={x:-9999,y:-9999},t0=performance.now();
let nodes=[],particles=[],stars=[],orbs=[];
function resize(){w=canvas.width=innerWidth;h=canvas.height=innerHeight;initMode()}
addEventListener('resize',resize);
addEventListener('mousemove',e=>{mouse.x=e.clientX;mouse.y=e.clientY});
addEventListener('mouseleave',()=>{mouse.x=-9999;mouse.y=-9999});
function initMode(){
  nodes=[];particles=[];stars=[];orbs=[];
  if(mode===0){const g=52;for(let y=0;y<Math.ceil(h/g)+1;y++)for(let x=0;x<Math.ceil(w/g)+1;x++)nodes.push({ox:x*g,oy:y*g,x:x*g,y:y*g,phase:Math.random()*6.28})}
  if(mode===1){for(let i=0;i<95;i++)particles.push({x:Math.random()*w,y:Math.random()*h,vx:(Math.random()-.5)*.45,vy:(Math.random()-.5)*.45,r:Math.random()*2+.5})}
  if(mode===2){for(let i=0;i<5;i++)nodes.push({phase:Math.random()*6.28,speed:.25+Math.random().*.4,amp:35+Math.random()*55,y:h*(.18+i*.16)})}
  if(mode===3){const g=40;for(let y=0;y<Math.ceil(h/g)+1;y++)for(let x=0;x<Math.ceil(w/g)+1;x++)nodes.push({ox:x*g+(y%2?g/2:0),oy:y*g*.86,x:0,y:0,phase:Math.random()*6.28})}
  if(mode===4){for(let i=0;i<130;i++)stars.push({x:Math.random()*w,y:Math.random()*h,z:Math.random()*1.5+.3,s:Math.random()*1.6+.3})}
  if(mode===5){for(let i=0;i<20;i++)orbs.push({x:Math.random()*w,y:Math.random()*h,r:18+Math.random()*42,vx:(Math.random()-.5)*.32,vy:(Math.random()-.5)*.32,phase:Math.random()*6.28})}
}
function draw(now){
  const t=(now-t0)*0.001;ctx.clearRect(0,0,w,h);
  if(mode===0){
    for(const n of nodes){n.x=n.ox+Math.sin(t*.65+n.phase)*5;n.y=n.oy+Math.cos(t*.5+n.phase*1.2)*4;const dx=n.x-mouse.x,dy=n.y-mouse.y,d=Math.sqrt(dx*dx+dy*dy);if(d<130){const f=(130-d)/130;n.x+=(dx/d)*f*24;n.y+=(dy/d)*f*24}}
    for(let i=0;i<nodes.length;i++){const a=nodes[i];for(let j=i+1;j<nodes.length;j++){const b=nodes[j],dx=a.x-b.x,dy=a.y-b.y,d=Math.sqrt(dx*dx+dy*dy);if(d<68){ctx.beginPath();ctx.strokeStyle=`rgba(255,0,51,${.12*(1-d/68)})`;ctx.lineWidth=.6;ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.stroke()}}}
    for(const n of nodes){const dx=n.x-mouse.x,dy=n.y-mouse.y,d=Math.sqrt(dx*dx+dy*dy),near=d<130;ctx.beginPath();ctx.arc(n.x,n.y,near?2.1:1.2,0,6.28);ctx.fillStyle=near?'rgba(255,40,80,.9)':'rgba(255,0,51,.3)';ctx.fill()}
  }
  if(mode===1){
    for(const p of particles){p.x+=p.vx;p.y+=p.vy;if(p.x<0||p.x>w)p.vx*=-1;if(p.y<0||p.y>h)p.vy*=-1;const dx=p.x-mouse.x,dy=p.y-mouse.y,d=Math.sqrt(dx*dx+dy*dy);if(d<100){p.vx-=(dx/d)*.15;p.vy-=(dy/d)*.15}ctx.beginPath();ctx.arc(p.x,p.y,p.r,0,6.28);ctx.fillStyle='rgba(255,0,51,.55)';ctx.fill()}
    for(let i=0;i<particles.length;i++)for(let j=i+1;j<particles.length;j++){const a=particles[i],b=particles[j],dx=a.x-b.x,dy=a.y-b.y,d=Math.sqrt(dx*dx+dy*dy);if(d<90){ctx.beginPath();ctx.strokeStyle=`rgba(255,0,51,${.1*(1-d/90)})`;ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.stroke()}}
  }
  if(mode===2){
    for(const b of nodes){ctx.beginPath();for(let x=0;x<=w;x+=6){const y=b.y+Math.sin(x*.008+t*b.speed+b.phase)*b.amp+Math.sin(x*.003-t*.2)*20;const dx=x-mouse.x,dy=y-mouse.y,d=Math.sqrt(dx*dx+dy*dy);const boost=d<160?(160-d)/160*25:0;x===0?ctx.moveTo(x,y-boost):ctx.lineTo(x,y-boost)}ctx.strokeStyle='rgba(255,0,51,.22)';ctx.lineWidth=2;ctx.stroke()}
  }
  if(mode===3){
    for(const n of nodes){n.x=n.ox+Math.sin(t*.5+n.phase)*3;n.y=n.oy+Math.cos(t*.4+n.phase)*3;const dx=n.x-mouse.x,dy=n.y-mouse.y,d=Math.sqrt(dx*dx+dy*dy);if(d<120){const f=(120-d)/120;n.x+=(dx/d)*f*18;n.y+=(dy/d)*f*18}ctx.beginPath();ctx.arc(n.x,n.y,d<120?2.4:1.3,0,6.28);ctx.fillStyle=d<120?'rgba(255,50,90,.85)':'rgba(255,0,51,.28)';ctx.fill()}
  }
  if(mode===4){
    for(const s of stars){s.x+=s.z*.15;if(s.x>w)s.x=0;const dx=s.x-mouse.x,dy=s.y-mouse.y,d=Math.sqrt(dx*dx+dy*dy);const bright=d<100?1:.4;ctx.beginPath();ctx.arc(s.x,s.y,s.s,0,6.28);ctx.fillStyle=`rgba(255,${40+bright*80},${60+bright*40},${.3+bright*.5})`;ctx.fill()}
  }
  if(mode===5){
    for(const o of orbs){o.x+=o.vx;o.y+=o.vy;o.phase+=.01;if(o.x<0||o.x>w)o.vx*=-1;if(o.y<0||o.y>h)o.vy*=-1;const dx=o.x-mouse.x,dy=o.y-mouse.y,d=Math.sqrt(dx*dx+dy*dy);if(d<150){o.vx-=(dx/d)*.08;o.vy-=(dy/d)*.08}const r=o.r+Math.sin(o.phase)*6;const g=ctx.createRadialGradient(o.x,o.y,0,o.x,o.y,r);g.addColorStop(0,'rgba(255,0,51,.2)');g.addColorStop(1,'rgba(255,0,51,0)');ctx.beginPath();ctx.arc(o.x,o.y,r,0,6.28);ctx.fillStyle=g;ctx.fill()}
  }
  requestAnimationFrame(draw);
}
resize();requestAnimationFrame(draw);
document.getElementById('bgSelect').onchange=e=>{mode=+e.target.value;initMode()};

const $=id=>document.getElementById(id);
const stateEl=$('state'),dot=$('dot'),checkedEl=$('checked'),availableEl=$('available');
const intervalEl=$('interval'),uptimeEl=$('uptime'),stopBtn=$('stop'),clearBtn=$('clear');
const hitlist=$('hitlist'),hitsCount=$('hitsCount'),badge=$('badge'),toast=$('toast');
let startTs=0,hits=0;
function showToast(m){toast.textContent=m;toast.classList.add('show');setTimeout(()=>toast.classList.remove('show'),2800)}
function fmt(s){return `${String(Math.floor(s/60)).padStart(2,'0')}:${String(s%60).padStart(2,'0')}`}
function setRunning(on){stopBtn.disabled=!on;dot.classList.toggle('on',on);stateEl.style.color=on?'#4CE0A0':'#FF0033'}
stopBtn.onclick=async()=>{await fetch('/api/stop',{method:'POST'});setRunning(false);stateEl.textContent='STOPPED';showToast('Hunt stopped')};
clearBtn.onclick=()=>{hitlist.innerHTML='<li class="empty" id="empty">List cleared</li>';hits=0;hitsCount.textContent='0 hits';showToast('List cleared')};
const ws=new WebSocket((location.protocol==='https:'?'wss:':'ws:')+'//'+location.host+'/ws');
ws.onmessage=e=>{
  const d=JSON.parse(e.data);
  if(d.type==='hit'){
    const emptyEl=document.getElementById('empty');if(emptyEl)emptyEl.remove();
    const li=document.createElement('li');
    li.innerHTML=`<span class="name">${d.username}</span><span class="ms">${d.elapsed_ms} ms</span>`;
    hitlist.prepend(li);
    availableEl.textContent=d.available.toLocaleString();
    checkedEl.textContent=d.checked.toLocaleString();
    hits++;hitsCount.textContent=hits+' hits';showToast('Found: '+d.username);
  }
  if(d.type==='progress'||d.type==='hello'){
    checkedEl.textContent=(d.checked||0).toLocaleString();
    availableEl.textContent=(d.available||0).toLocaleString();
    if(d.interval)intervalEl.textContent=d.interval+'s';
    if(d.status){stateEl.textContent=d.status;stateEl.style.color=d.status==='HUNTING'?'#4CE0A0':'#FF0033'}
    if(d.running!==undefined)setRunning(d.running);
  }
  if(d.type==='status'){stateEl.textContent=d.status;stateEl.style.color=d.status==='HUNTING'?'#4CE0A0':'#F5BB58';if(d.status!=='HUNTING')setRunning(false)}
};
setInterval(()=>{if(startTs)uptimeEl.textContent=fmt(Math.floor((Date.now()-startTs)/1000))},1000);
fetch('/api/status').then(r=>r.json()).then(d=>{
  checkedEl.textContent=d.checked.toLocaleString();
  availableEl.textContent=d.available.toLocaleString();
  stateEl.textContent=d.status;
  if(d.webhook_configured){badge.textContent='WEBHOOK ON';badge.classList.remove('off')}
  else{badge.textContent='WEBHOOK OFF';badge.classList.add('off')}
  if(d.running){setRunning(true);startTs=Date.now()-d.uptime*1000;stateEl.textContent='HUNTING'}
});
</script>
</body>
</html>
"""


@app.on_event("startup")
async def startup():
    load_persistence()
    Path("templates/index.html").write_text(INDEX_HTML, encoding="utf-8")
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
