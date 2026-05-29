// server.js — SupKonQuest Game Server (relay Godot-compatible)
const WebSocket = require('ws');
const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });
const rooms = new Map();
const clientRoom = new Map();

// ── Compatibilité WebSocketMultiplayerPeer (Godot 4) ────────────────────────
// Godot attend, comme TOUT PREMIER message du serveur, 4 octets = un int32
// little-endian contenant le peer ID du client (≥ 2). Sans ce paquet, le peer
// reste bloqué en CONNECTION_CONNECTING. On l'envoie donc dès la connexion.
let peerIdCounter = 1;                 // le prochain ID attribué sera 2
const clientPeerId = new Map();        // WebSocket -> int (peer ID)
const roomPeerIds  = new Map();        // roomId -> Set<int> (peer IDs présents)

console.log(`SupKonQuest Game Server démarré sur port ${PORT}`);

wss.on('connection', (ws, req) => {
  const url    = new URL(req.url, 'http://localhost');
  const roomId = url.searchParams.get('room') || 'default';

  clientRoom.set(ws, roomId);
  if (!rooms.has(roomId)) rooms.set(roomId, new Set());
  rooms.get(roomId).add(ws);

  // Attribuer un peer ID (≥ 2) et l'envoyer immédiatement en binaire (int32 LE).
  const peerId = ++peerIdCounter;
  clientPeerId.set(ws, peerId);
  if (!roomPeerIds.has(roomId)) roomPeerIds.set(roomId, new Set());
  roomPeerIds.get(roomId).add(peerId);

  const buf = Buffer.allocUnsafe(4);
  buf.writeInt32LE(peerId, 0);
  ws.send(buf);

  console.log(`[+] nouveau client -> room "${roomId}" (peer ${peerId}, ${rooms.get(roomId).size} joueurs)`);

  // Relay pur : retransmettre tout message reçu à tous les autres peers.
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
