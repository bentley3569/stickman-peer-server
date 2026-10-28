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
app.get('/', (req, res) => res.send('Stickman Arena peer server is running'));
app.get('/health', (req, res) => res.json({ ok: true, time: Date.now() }));

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

peerServer.on('connection', c => console.log('connected   ', c.getId()));
peerServer.on('disconnect', c => console.log('disconnected', c.getId()));

server.listen(PORT, () => console.log('Peer server listening on port ' + PORT + ' (path /peerjs)'));
process.on('uncaughtException', e => console.error('uncaught', e));
process.on('unhandledRejection', e => console.error('unhandled', e));
