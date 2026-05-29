const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// rooms : { roomId: { clients: Set<ws> } }
const rooms = new Map();
// ws → roomId
const clientRoom = new Map();

console.log(`SupKonQuest Game Server démarré sur port ${PORT}`);

wss.on('connection', (ws, req) => {
  // Le room_id est passé dans l'URL : wss://serveur.com/?room=MaRoom
  const url    = new URL(req.url, 'http://localhost');
  const roomId = url.searchParams.get('room') || 'default';

  if (!rooms.has(roomId)) rooms.set(roomId, new Set());
  const room = rooms.get(roomId);
  room.add(ws);
  clientRoom.set(ws, roomId);
  console.log(`[+] Connecté -> room "${roomId}" (${room.size} joueurs)`);

  ws.on('message', (data, isBinary) => {
    // Relay pur : renvoyer à tous les autres dans la room
    for (const client of room) {
      if (client !== ws && client.readyState === WebSocket.OPEN) {
        client.send(data, { binary: isBinary });
      }
    }
  });

  ws.on('close', () => {
    room.delete(ws);
    clientRoom.delete(ws);
    console.log(`[-] Déconnecté de room "${roomId}" (${room.size} restants)`);
    if (room.size === 0) {
      rooms.delete(roomId);
      console.log(`Room "${roomId}" supprimée`);
    }
  });

  ws.on('error', (err) => console.error('[WS] Erreur :', err.message));
});
