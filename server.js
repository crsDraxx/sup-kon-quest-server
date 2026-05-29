// server.js — SupKonQuest Game Server (relay pur, sans handshake custom)
const WebSocket = require('ws');
const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });
const rooms = new Map();
const clientRoom = new Map();

console.log(`SupKonQuest Game Server démarré sur port ${PORT}`);

wss.on('connection', (ws, req) => {
  const url    = new URL(req.url, 'http://localhost');
  const roomId = url.searchParams.get('room') || 'default';

  clientRoom.set(ws, roomId);
  if (!rooms.has(roomId)) rooms.set(roomId, new Set());
  rooms.get(roomId).add(ws);

  console.log(`[+] nouveau client -> room "${roomId}" (${rooms.get(roomId).size} joueurs)`);

  // PAS de handshake binaire ici — Godot gère son propre protocole.
  // Le serveur est un relay pur : il retransmet tout à tous les autres peers.

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
    clientRoom.delete(ws);
    console.log(`[-] client déconnecté de room "${roomId}"`);
  });

  ws.on('error', (err) => console.error(`[WS] Erreur:`, err.message));
});
