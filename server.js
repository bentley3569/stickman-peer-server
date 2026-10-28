// Stickman Arena – self-hosted PeerJS signaling server.
// It only introduces players to each other; the fight itself is peer-to-peer.
const express = require('express');
const cors = require('cors');
const http = require('http');
const { ExpressPeerServer } = require('peer');

const PORT = process.env.PORT || 9000;
const app = express();
app.use(cors());                                   // allow the game to be hosted anywhere

// Health check + keep-alive target (point UptimeRobot / cron-job.org here every 5 min)
app.get('/health', (req, res) => res.json({ ok: true, time: Date.now() }));

// Live player counts (so "N ONLINE" and queue sizes are always right, 24/7)
const online = new Set();                           // ids of logged-in players ("stickman_user_...")
const lobbies = new Set();                          // waiting quick/ranked lobbies
const isLobby = id => id === 'stickman_quick_lobby' || id.indexOf('stickman_ranked_') === 0;
app.get('/online', (req, res) => {
  const q = {}; lobbies.forEach(id => { q[id] = 1; });
  res.set('Cache-Control', 'no-store').json({ n: online.size, q });
});

// Serve the game itself from this server too (optional): put stickman-arena.html here as index.html
const path = require('path');
const fs = require('fs');
app.get('/', (req, res, next) => {
  const f = path.join(__dirname, 'index.html');
  if (fs.existsSync(f)) return res.sendFile(f);
  next();
});

const server = http.createServer(app);
const peerServer = ExpressPeerServer(server, {
  path: '/',
  allow_discovery: false,
  proxied: true,                                   // required behind Render/Fly/Railway proxies
  alive_timeout: 60000,
  key: 'peerjs',
  concurrent_limit: 5000
});
app.use('/peerjs', peerServer);
app.get('/', (req, res) => res.send('Stickman Arena peer server is running'));   // fallback when no index.html is present

peerServer.on('connection', c => {
  const id = c.getId();
  if (id.indexOf('stickman_user_') === 0) online.add(id);
  if (isLobby(id)) lobbies.add(id);
});
peerServer.on('disconnect', c => {
  const id = c.getId();
  online.delete(id); lobbies.delete(id);
});

server.listen(PORT, () => console.log('Peer server listening on port ' + PORT + ' (path /peerjs)'));
process.on('uncaughtException', e => console.error('uncaught', e));
process.on('unhandledRejection', e => console.error('unhandled', e));
