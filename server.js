/* Stickman Arena Online - matchmaking + relay server.
   Players connect with a WebSocket to /ws. The server pairs them (quick match, ranked, or room code),
   then relays each player's messages to the other. It also counts who is online / in each queue.
   Run:  npm install && npm start   (uses $PORT, default 8080) */
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const fs = require('fs');
const path = require('path');
const GAME = path.join(__dirname, 'index.html');   // if the game file sits next to this server, players can just open the site URL
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); return; }
  if (url === '/' || url === '/index.html') {
    fs.readFile(GAME, (err, data) => {
      if (err) { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' }); res.end(data);
    });
    return;
  }
  res.writeHead(404); res.end();
});
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 8192 });

const clients = new Set();
const queues = new Map();   // queue key -> [client, ...] waiting
const rooms = new Map();    // room code -> hosting client
const KEY_RE = /^stickman_[A-Za-z0-9_]{1,40}$/;
const CODE_RE = /^[A-Z0-9]{4}$/;

const newId = () => crypto.randomBytes(5).toString('hex');
function send(c, o) { if (c.ws.readyState === 1) { try { c.ws.send(JSON.stringify(o)); } catch (e) {} } }

function leaveAll(c) {   // remove from queue / room / current match, telling the opponent
  if (c.qkey) { const q = queues.get(c.qkey); if (q) { const i = q.indexOf(c); if (i >= 0) q.splice(i, 1); if (!q.length) queues.delete(c.qkey); } c.qkey = null; }
  if (c.room) { if (rooms.get(c.room) === c) rooms.delete(c.room); c.room = null; }
  if (c.opp) { const o = c.opp; c.opp = null; if (o.opp === c) { o.opp = null; send(o, { t: 'oppleft' }); } }
}
function pair(a, b) {
  leaveAll(a); leaveAll(b);   // clears queue/room state first
  a.opp = b; b.opp = a;
  send(a, { t: 'match', opp: b.id });
  send(b, { t: 'match', opp: a.id });
}
function counts() {
  const q = {}; for (const [k, v] of queues) q[k] = v.length;
  return { t: 'pc', n: clients.size, q };
}

wss.on('connection', ws => {
  const c = { ws, id: newId(), opp: null, qkey: null, room: null, alive: true, msgs: 0 };
  clients.add(c);
  send(c, { t: 'id', id: c.id }); send(c, counts());
  ws.on('pong', () => { c.alive = true; });
  ws.on('message', raw => {
    if (++c.msgs > 500) { ws.close(1008, 'rate'); return; }   // 500 msgs/s is far above normal (~70/s)
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    switch (m.t) {
      case 'rel': if (c.opp && m.d && typeof m.d === 'object') send(c.opp, { t: 'rel', d: m.d }); break;
      case 'q': {
        if (typeof m.key !== 'string' || !KEY_RE.test(m.key)) return;
        leaveAll(c);
        const q = queues.get(m.key) || [];
        let other = null;
        while (q.length) { const x = q.shift(); if (x !== c && x.ws.readyState === 1 && x.qkey === m.key) { other = x; break; } }
        if (!q.length) queues.delete(m.key); else queues.set(m.key, q);
        if (other) pair(other, c);
        else { c.qkey = m.key; const q2 = queues.get(m.key) || []; q2.push(c); queues.set(m.key, q2); }
        break;
      }
      case 'host': {
        leaveAll(c);
        let code, tries = 0;
        do { code = Array.from(crypto.randomBytes(4), b => 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'[b % 32]).join(''); } while (rooms.has(code) && ++tries < 20);
        rooms.set(code, c); c.room = code; send(c, { t: 'code', code });
        break;
      }
      case 'join': {
        const code = typeof m.code === 'string' ? m.code.toUpperCase() : '';
        const host = CODE_RE.test(code) ? rooms.get(code) : null;
        if (!host || host === c || host.opp || host.ws.readyState !== 1) { send(c, { t: 'err', e: 'nocode' }); return; }
        rooms.delete(code); host.room = null; pair(host, c);
        break;
      }
      case 'leave': leaveAll(c); break;
    }
  });
  const gone = () => { leaveAll(c); clients.delete(c); };
  ws.on('close', gone); ws.on('error', () => {});
});

setInterval(() => { for (const c of clients) c.msgs = 0; }, 1000);
setInterval(() => {   // heartbeat: drop dead sockets (also keeps the connection alive through proxies)
  for (const c of clients) { if (!c.alive) { try { c.ws.terminate(); } catch (e) {} continue; } c.alive = false; try { c.ws.ping(); } catch (e) {} }
}, 25000);
setInterval(() => { const p = counts(); for (const c of clients) send(c, p); }, 2000);

server.listen(PORT, () => console.log('Stickman Arena server listening on ' + PORT));
