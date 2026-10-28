'use strict';
/* Stickman Arena game server (WebSocket).
   - quick match / ranked queues, 4-letter room codes, relay between the two fighters
   - online + queue counters
   - developer tools: live-match feed, spectate toys, polls, gifting coins
   Deploy: `npm install` then `npm start`. Optional env: PORT, DEV_KEY (plain dev passcode; if unset the passcode hash below is used). */
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const DEV_KEY = process.env.DEV_KEY || '';
const DEV_HASH = 5178711972820204, DEV_SALT = 'sa_dev_v1:';
function cyrb53(s, seed = 0) {
  let h1 = 0xdeadbeef ^ seed, h2 = 0x41c6ce57 ^ seed;
  for (let i = 0, c; i < s.length; i++) { c = s.charCodeAt(i); h1 = Math.imul(h1 ^ c, 2654435761); h2 = Math.imul(h2 ^ c, 1597334677); }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  return 4294967296 * (2097151 & h2) + (h1 >>> 0);
}
function devKeyOK(k) {
  if (typeof k !== 'string' || !k || k.length > 100) return false;
  if (DEV_KEY) { const a = Buffer.from(k), b = Buffer.from(DEV_KEY); return a.length === b.length && crypto.timingSafeEqual(a, b); }
  return cyrb53(DEV_SALT + k) === DEV_HASH;
}

const server = http.createServer((req, res) => {
  res.writeHead(req.url === '/health' || req.url === '/' ? 200 : 404, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
  res.end(req.url === '/health' || req.url === '/' ? 'ok' : 'not found');
});
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 });

const clients = new Set();
const queues = new Map();     // key -> waiting client
const rooms = new Map();      // code -> hosting client
const gifts = new Map();      // gift id -> { dev, user, t }
let poll = null;              // { id, q, opts, votes: Map(vid -> option), active }
let watching = false;

const send = (c, o) => { if (c && c.ws.readyState === 1) { try { c.ws.send(JSON.stringify(o)); } catch (e) {} } };
const devs = () => [...clients].filter(c => c.dev);
const uid = () => crypto.randomBytes(6).toString('hex');
const str = (v, n) => typeof v === 'string' ? v.replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, n) : '';

function updateWatch() {
  const on = devs().length > 0;
  if (on !== watching) { watching = on; clients.forEach(c => send(c, { t: 'watch', on })); }
}
function pollStat() {
  if (!poll) return null;
  const counts = poll.opts.map(() => 0); poll.votes.forEach(o => { counts[o]++; });
  return { t: 'pollstat', id: poll.id, q: poll.q, opts: poll.opts, counts, active: poll.active };
}
function leave(c) {
  if (c.key && queues.get(c.key) === c) queues.delete(c.key); c.key = '';
  if (c.room && rooms.get(c.room) === c) rooms.delete(c.room); c.room = '';
  if (c.opp) { const o = c.opp; c.opp = null; if (o.opp === c) { o.opp = null; send(o, { t: 'oppleft' }); } }
}
function pair(a, b) { a.opp = b; b.opp = a; a.key = b.key = ''; send(a, { t: 'match', opp: b.id }); send(b, { t: 'match', opp: a.id }); }
function newCode() { const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; for (let n = 0; n < 50; n++) { let s = ''; for (let i = 0; i < 4; i++) s += A[Math.floor(Math.random() * A.length)]; if (!rooms.has(s)) return s; } return ''; }

wss.on('connection', ws => {
  if (clients.size > 3000) { ws.close(); return; }
  const c = { ws, id: uid(), key: '', room: '', opp: null, un: '', vid: '', dev: false, fails: 0, alive: true, win: 0, cnt: 0 };
  clients.add(c);
  send(c, { t: 'id', id: c.id }); send(c, { t: 'watch', on: watching });
  if (poll && poll.active) send(c, { t: 'poll', id: poll.id, q: poll.q, opts: poll.opts });
  ws.on('pong', () => { c.alive = true; });
  ws.on('message', raw => {
    const now = Date.now(); if (now - c.win > 1000) { c.win = now; c.cnt = 0; } if (++c.cnt > 400) return;   // flood guard
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    switch (m.t) {
      case 'rel': if (c.opp && m.d && typeof m.d === 'object') send(c.opp, { t: 'rel', d: m.d }); break;
      case 'leave': leave(c); break;
      case 'q': {
        const key = str(m.key, 50); if (key.indexOf('stickman_') !== 0) break; leave(c);
        const w = queues.get(key);
        if (w && w !== c && w.ws.readyState === 1) { queues.delete(key); pair(w, c); } else { queues.set(key, c); c.key = key; }
        break;
      }
      case 'host': { leave(c); const code = newCode(); if (!code) break; rooms.set(code, c); c.room = code; send(c, { t: 'code', code }); break; }
      case 'join': {
        const code = str(m.code, 8).toUpperCase(), h = rooms.get(code); leave(c);
        if (!h || h === c || h.ws.readyState !== 1) { send(c, { t: 'err' }); break; }
        rooms.delete(code); h.room = ''; pair(h, c); break;
      }
      case 'reg': { const u = str(m.un, 16).toLowerCase(); c.un = /^[a-z0-9_]{3,16}$/.test(u) ? u : ''; c.vid = str(m.vid, 40); break; }
      case 'giftack': {
        const g = gifts.get(str(m.id, 40)); if (!g || g.user !== c.un) break; gifts.delete(str(m.id, 40));
        send(g.dev, { t: 'giftres', id: str(m.id, 40), ok: 1, name: str(m.name, 16) || c.un }); break;
      }
      case 'v': {   // a player votes
        if (!poll || !poll.active || m.id !== poll.id) break;
        const vid = str(m.vid, 40), o = Math.floor(+m.o);
        if (!vid || !isFinite(o) || o < 0 || o >= poll.opts.length || poll.votes.has(vid) || poll.votes.size > 20000) break;
        poll.votes.set(vid, o); const s = pollStat(); devs().forEach(d => send(d, s)); break;
      }
      case 'rep': {   // live fighter state from a player in a match -> developers
        if (!watching || !m.s || typeof m.s !== 'object' || m.pid !== c.id || typeof m.mid !== 'string' || m.mid.length > 120 || m.mid.split('|').indexOf(c.id) < 0) break;
        const d = { t: 'r', mid: m.mid, pid: c.id, rk: !!m.rk, nm: str(m.nm, 16), un: str(m.un, 16), lv: m.lv, rt: m.rt, s: m.s };
        devs().forEach(dv => send(dv, { t: 'dr', d })); break;
      }
      case 'devauth': {
        if (devKeyOK(m.key)) { c.dev = true; c.fails = 0; send(c, { t: 'devok' }); const s = pollStat(); if (s) send(c, s); updateWatch(); }
        else { send(c, { t: 'deverr' }); if (++c.fails >= 5) ws.close(); }
        break;
      }
      case 'devoff': c.dev = false; updateWatch(); break;
      default: if (c.dev) devCmd(c, m);
    }
  });
  ws.on('close', () => { leave(c); clients.delete(c); if (c.dev) updateWatch(); });
  ws.on('error', () => {});
});

function devCmd(c, m) {
  switch (m.t) {
    case 'dcmd': {   // message / boost / heal to the players of a match
      const ids = Array.isArray(m.to) ? m.to.slice(0, 2).map(x => str(x, 40)) : [], d = m.d && typeof m.d === 'object' ? m.d : null;
      let n = 0; if (d) clients.forEach(p => { if (ids.indexOf(p.id) >= 0) { send(p, { t: 'dcmd', d }); n++; } });
      send(c, { t: 'dcres', n }); break;
    }
    case 'gift': {
      const id = str(m.id, 40), user = str(m.user, 16).toLowerCase(), amt = Math.floor(+m.amt);
      if (!id || !isFinite(amt) || amt < 1 || amt > 100000) break;
      const target = [...clients].find(p => p.un && p.un === user);
      if (!target) { send(c, { t: 'giftres', id, ok: 0, why: 'is not online right now.' }); break; }
      gifts.set(id, { dev: c, user, t: Date.now() }); send(target, { t: 'gift', id, amt }); break;
    }
    case 'pollset': {
      const id = str(m.id, 40), q = str(m.q, 80), opts = Array.isArray(m.opts) ? m.opts.slice(0, 4).map(o => str(o, 30)).filter(Boolean) : [];
      if (!id || !q || opts.length < 2) break;
      poll = { id, q, opts, votes: new Map(), active: true };
      clients.forEach(p => send(p, { t: 'poll', id, q, opts }));
      devs().forEach(d => { send(d, { t: 'pollres', n: clients.size }); send(d, pollStat()); }); break;
    }
    case 'pollend': {
      if (!poll || !poll.active || m.id !== poll.id) break; poll.active = false;
      const s = pollStat(); clients.forEach(p => send(p, { t: 'pollend', id: poll.id, counts: s.counts })); devs().forEach(d => send(d, s)); break;
    }
  }
}

setInterval(() => {   // online counter + queue sizes
  const q = {}; queues.forEach((c, k) => { q[k] = 1; });
  const msg = { t: 'pc', n: clients.size, q };
  clients.forEach(c => send(c, msg));
  const now = Date.now(); gifts.forEach((g, k) => { if (now - g.t > 20000) gifts.delete(k); });
}, 2000);
setInterval(() => { clients.forEach(c => { if (!c.alive) { try { c.ws.terminate(); } catch (e) {} return; } c.alive = false; try { c.ws.ping(); } catch (e) {} }); }, 30000);

server.listen(PORT, () => console.log('Stickman game server on :' + PORT));
