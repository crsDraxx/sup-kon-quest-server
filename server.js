// server.js — SupKonQuest Game Server (relay Godot-compatible)
const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;

// Serveur HTTP pour le keep-alive UptimeRobot
const server = http.createServer((req, res) => {
  if (req.url === '/ping') {
    res.writeHead(200);
    res.end('pong');
  } else {
    res.writeHead(200);
    res.end('SupKonQuest Server OK');
  }
});

const wss = new WebSocket.Server({ server });

const rooms = new Map();
const clientRoom = new Map();
let peerIdCounter = 1;
const clientPeerId = new Map();
const roomPeerIds  = new Map();

wss.on('connection', (ws, req) => {
  const url    = new URL(req.url, 'http://localhost');
  const roomId = url.searchParams.get('room') || 'default';
  clientRoom.set(ws, roomId);
  if (!rooms.has(roomId)) rooms.set(roomId, new Set());
  rooms.get(roomId).add(ws);

  const peerId = ++peerIdCounter;
  clientPeerId.set(ws, peerId);
  if (!roomPeerIds.has(roomId)) roomPeerIds.set(roomId, new Set());
  roomPeerIds.get(roomId).add(peerId);

  const buf = Buffer.allocUnsafe(4);
  buf.writeInt32LE(peerId, 0);
  ws.send(buf);

  console.log(`[+] nouveau client -> room "${roomId}" (peer ${peerId}, ${rooms.get(roomId).size} joueurs)`);

  ws.on('message', (data, isBinary) => {
    const room = rooms.get(clientRoom.get(ws));
    if (!room) return;
    for (const client of room) {
      if (client !== ws && client.readyState === WebSocket.OPEN) {
        client.send(data, { binary: isBinary });
      }
    }
  });

  ws.on('close', () => {
    const room = rooms.get(roomId);
    if (room) {
      room.delete(ws);
      if (room.size === 0) {
        rooms.delete(roomId);
        console.log(`Room "${roomId}" supprimée`);
      }
    }
    const pid = clientPeerId.get(ws);
    const set = roomPeerIds.get(roomId);
    if (set && pid !== undefined) {
      set.delete(pid);
      if (set.size === 0) roomPeerIds.delete(roomId);
    }
    clientPeerId.delete(ws);
    clientRoom.delete(ws);
    console.log(`[-] client déconnecté de room "${roomId}" (peer ${pid})`);
  });

  ws.on('error', (err) => console.error(`[WS] Erreur:`, err.message));
});

server.listen(PORT, () => {
  console.log(`SupKonQuest Game Server démarré sur port ${PORT}`);
});
