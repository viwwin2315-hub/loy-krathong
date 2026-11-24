// เซิร์ฟเวอร์ลอยกระทงออนไลน์: เสิร์ฟหน้าเว็บ + รีเลย์ตำแหน่งผู้เล่นผ่าน WebSocket
// รัน: npm install && npm start   (ค่าเริ่มต้นพอร์ต 3000, รับได้ 200 คน)
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_CLIENTS = +process.env.MAX_CLIENTS || 200;
const ALLOWED = new Set(['x', 'y', 'fx', 'fy', 'mv', 'L', 'n', 'lk']);

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(fs.readFileSync(path.join(__dirname, 'index.html')));
  } else {
    res.writeHead(404);
    res.end('not found');
  }
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 8192 });
const clients = new Map();
let nextId = 1;
let pending = {};

function send(ws, o) {
  if (ws.readyState === 1) ws.send(JSON.stringify(o));
}

wss.on('connection', (ws) => {
  if (clients.size >= MAX_CLIENTS) { ws.close(1013, 'full'); return; }
  const id = 'p' + nextId++;
  const c = { ws, state: {}, tokens: 40, last: Date.now(), alive: true };
  clients.set(id, c);

  const snap = {};
  clients.forEach((v, k) => { if (k !== id) snap[k] = v.state; });
  send(ws, { t: 'hello', id, peers: snap });

  ws.on('pong', () => { c.alive = true; });
  ws.on('message', (buf) => {
    const now = Date.now();
    c.tokens = Math.min(40, c.tokens + ((now - c.last) / 1000) * 30);
    c.last = now;
    if (c.tokens < 1) return;
    c.tokens--;
    let m;
    try { m = JSON.parse(buf.toString()); } catch (e) { return; }
    if (!m || m.t !== 'p' || !m.d || typeof m.d !== 'object' || Array.isArray(m.d)) return;
    const patch = {};
    for (const k of Object.keys(m.d)) if (ALLOWED.has(k)) patch[k] = m.d[k];
    const next = Object.assign({}, c.state);
    for (const k of Object.keys(patch)) {
      if (patch[k] === null) delete next[k]; else next[k] = patch[k];
    }
    if (JSON.stringify(next).length > 4096) return;
    c.state = next;
    pending[id] = Object.assign(pending[id] || {}, patch);
  });
  ws.on('close', () => {
    clients.delete(id);
    delete pending[id];
    clients.forEach((v) => send(v.ws, { t: 'l', id }));
  });
});

// ส่งการเปลี่ยนแปลงรวมกลุ่มทุก 100 มิลลิวินาที เพื่อลดภาระเมื่อมีผู้เล่นเยอะ
setInterval(() => {
  const ids = Object.keys(pending);
  if (!ids.length) return;
  clients.forEach((v, id) => {
    const u = {};
    let any = false;
    for (const k of ids) if (k !== id) { u[k] = pending[k]; any = true; }
    if (any) send(v.ws, { t: 'b', u });
  });
  pending = {};
}, 100);

// ตัดการเชื่อมต่อที่ค้าง
setInterval(() => {
  clients.forEach((c) => {
    if (!c.alive) { c.ws.terminate(); return; }
    c.alive = false;
    try { c.ws.ping(); } catch (e) {}
  });
}, 30000);

server.listen(PORT, () => console.log('ลอยกระทงออนไลน์ทำงานที่ http://localhost:' + PORT));
