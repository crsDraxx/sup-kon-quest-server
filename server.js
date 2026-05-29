// server.js — SupKonQuest Game Server
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

let rooms = {}; // { roomId: { host, clients: [ws, ...], state: {} } }
let clientIds = new Map(); // ws → peerId

let nextPeerId = 1;

console.log(`SupKonQuest Game Server démarré sur port ${PORT}`);

wss.on('connection', (ws) => {
  const peerId = nextPeerId++;
  clientIds.set(ws, peerId);
  console.log(`[+] Joueur connecté : peer_${peerId}`);

  // Godot WebSocketMultiplayerPeer attend l'ID en 4 bytes big-endian
  // suivi du type de message (1 = ID assignment)
  const buf = Buffer.alloc(8);
  buf.writeUInt32BE(peerId, 0);   // peer_id
  buf.writeUInt32BE(0, 4);        // target (0 = broadcast)
  ws.send(buf);

  ws.on('message', (raw) => {
    // Messages Godot sont binaires — extraire le payload JSON
    let msg;
    try {
      const str = raw.toString('utf8');
      msg = JSON.parse(str);
    } catch {
      // Message binaire Godot natif — relayer directement
      for (const [client] of clientIds) {
        if (client !== ws && client.readyState === WebSocket.OPEN) {
          client.send(raw);
        }
      }
      return;
    }
    handleMessage(ws, peerId, msg);
  });

  ws.on('close', () => {
    console.log(`[-] Joueur déconnecté : peer_${peerId}`);
    handleDisconnect(ws, peerId);
    clientIds.delete(ws);
  });

  ws.on('error', (err) => console.error(`Erreur peer_${peerId}:`, err));
});

function handleMessage(ws, peerId, msg) {
  switch (msg.type) {

    case 'join_room': {
      const { room_id } = msg;
      if (!rooms[room_id]) {
        rooms[room_id] = { host: peerId, clients: [], state: {} };
      }
      const room = rooms[room_id];
      room.clients.push(ws);

      // Notifier les autres joueurs
      const othersIds = room.clients
        .filter(c => c !== ws)
        .map(c => clientIds.get(c));

      // Dire au nouveau qui est déjà là
      send(ws, { type: 'room_peers', peers: othersIds, host: room.host });

      // Dire aux autres qu'un nouveau est arrivé
      broadcast(room, ws, { type: 'peer_joined', peer_id: peerId });

      console.log(`peer_${peerId} rejoint room ${room_id} (${room.clients.length} joueurs)`);
      break;
    }

    case 'leave_room': {
      handleDisconnect(ws, peerId);
      break;
    }

    case 'rpc': {
      // Relay RPC : { type:'rpc', room_id, target, method, args }
      const room = findRoom(ws);
      if (!room) return;

      if (msg.target === 0) {
        // Broadcast à tous sauf l'émetteur
        broadcast(room, ws, { ...msg, from: peerId });
      } else {
        // Envoyer à un peer spécifique
        const targetWs = findPeerWs(msg.target);
        if (targetWs) send(targetWs, { ...msg, from: peerId });
      }
      break;
    }

    case 'game_state': {
      // Synchronisation d'état (host → tous)
      const room = findRoom(ws);
      if (!room || room.host !== peerId) return;
      room.state = msg.state;
      broadcast(room, ws, { type: 'game_state', state: msg.state, from: peerId });
      break;
    }

    case 'ping':
      send(ws, { type: 'pong' });
      break;
  }
}

function handleDisconnect(ws, peerId) {
  for (const [roomId, room] of Object.entries(rooms)) {
    const idx = room.clients.indexOf(ws);
    if (idx === -1) continue;

    room.clients.splice(idx, 1);
    broadcast(room, null, { type: 'peer_left', peer_id: peerId });

    if (room.clients.length === 0) {
      delete rooms[roomId];
      console.log(`Room ${roomId} supprimée (vide)`);
    } else if (room.host === peerId) {
      // Transférer le host au premier client restant
      room.host = clientIds.get(room.clients[0]);
      broadcast(room, null, { type: 'host_changed', new_host: room.host });
      console.log(`Host de ${roomId} transféré à peer_${room.host}`);
    }
    break;
  }
}

function findRoom(ws) {
  return Object.values(rooms).find(r => r.clients.includes(ws)) || null;
}

function findPeerWs(peerId) {
  for (const [ws, id] of clientIds) {
    if (id === peerId) return ws;
  }
  return null;
}

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN)
    ws.send(JSON.stringify(obj));
}

function broadcast(room, excludeWs, obj) {
  for (const client of room.clients) {
    if (client !== excludeWs) send(client, obj);
  }
}
