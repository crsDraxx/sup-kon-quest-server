// server.js — SupKonQuest Game Server
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

const rooms = new Map();
const clientRoom = new Map();
const clientIds = new Map();
let nextPeerId = 2; // 1 est réservé au "serveur" côté Godot

console.log(`SupKonQuest Game Server démarré sur port ${PORT}`);

wss.on('connection', (ws, req) => {
  const url    = new URL(req.url, 'http://localhost');
  const roomId = url.searchParams.get('room') || 'default';
  const peerId = nextPeerId++;

  clientIds.set(ws, peerId);
  clientRoom.set(ws, roomId);

  if (!rooms.has(roomId)) rooms.set(roomId, new Set());
  rooms.get(roomId).add(ws);

  console.log(`[+] peer_${peerId} -> room "${roomId}" (${rooms.get(roomId).size} joueurs)`);

  // ── Handshake Godot WebSocketMultiplayerPeer ──────────────────────────────
  // Format attendu par websocket_multiplayer_peer.cpp :
  // 4 bytes : peer_id (big-endian uint32)
  // Ce message dit au client "ton ID est X"
  const handshake = Buffer.alloc(4);
  handshake.writeUInt32BE(peerId, 0);
  ws.send(handshake);

  ws.on('message', (data, isBinary) => {
    const room = rooms.get(clientRoom.get(ws));
    if (!room) return;
    // Relay binaire brut à tous les autres peers de la room
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
    clientIds.delete(ws);
    clientRoom.delete(ws);
    console.log(`[-] peer_${peerId} déconnecté de room "${roomId}"`);
  });

  ws.on('error', (err) => console.error(`[WS] Erreur peer_${peerId}:`, err.message));
});
