const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static(__dirname));

// rooms: { [roomCode]: { users: { [userId]: { name, budget, items } } } }
const rooms = {};
// clientMeta: Map<ws, { roomCode, userId }>
const clientMeta = new Map();

function ensureRoom(roomCode) {
  if (!rooms[roomCode]) rooms[roomCode] = { users: {}, menu: { customItems: [], overrides: {}, hidden: [] } };
  return rooms[roomCode];
}

function broadcastToRoom(roomCode, exceptWs, data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(ws => {
    const meta = clientMeta.get(ws);
    if (ws !== exceptWs && ws.readyState === WebSocket.OPEN && meta?.roomCode === roomCode) {
      ws.send(msg);
    }
  });
}

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}

wss.on('connection', (ws) => {
  clientMeta.set(ws, { roomCode: null, userId: null });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const meta = clientMeta.get(ws);

    if (msg.type === 'join') {
      const { roomCode, userId, name, budget } = msg;
      meta.roomCode = roomCode;
      meta.userId = userId;
      const room = ensureRoom(roomCode);
      room.users[userId] = { name, budget, items: {} };
      send(ws, { type: 'state', room });
      broadcastToRoom(roomCode, ws, { type: 'userJoined', userId, user: room.users[userId] });
    }

    else if (msg.type === 'updateItems') {
      const { roomCode, userId } = meta;
      if (!roomCode || !userId || !rooms[roomCode]?.users[userId]) return;
      rooms[roomCode].users[userId].items = msg.items;
      broadcastToRoom(roomCode, ws, { type: 'itemsUpdated', userId, items: msg.items });
    }

    else if (msg.type === 'updateBudget') {
      const { roomCode, userId } = meta;
      if (!roomCode || !userId || !rooms[roomCode]?.users[userId]) return;
      rooms[roomCode].users[userId].budget = msg.budget;
      broadcastToRoom(roomCode, ws, { type: 'budgetUpdated', userId, budget: msg.budget });
    }

    else if (msg.type === 'updateMenu') {
      const { roomCode } = meta;
      if (!roomCode || !rooms[roomCode]) return;
      rooms[roomCode].menu = msg.menu;
      broadcastToRoom(roomCode, ws, { type: 'menuUpdated', menu: msg.menu });
    }
  });

  ws.on('close', () => {
    const { roomCode, userId } = clientMeta.get(ws) || {};
    if (roomCode && userId && rooms[roomCode]) {
      delete rooms[roomCode].users[userId];
      broadcastToRoom(roomCode, ws, { type: 'userLeft', userId });
      if (Object.keys(rooms[roomCode].users).length === 0) delete rooms[roomCode];
    }
    clientMeta.delete(ws);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🍞 성심당 서버 실행 중: http://localhost:${PORT}`);
  console.log('   친구에게 링크를 공유하세요!');
});
