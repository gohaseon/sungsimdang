const express = require('express');
const WebSocket = require('ws');
const http = require('http');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static(__dirname));

// ==================== REDIS ====================
let redis = null;
try {
  if (process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN) {
    const { Redis } = require('@upstash/redis');
    redis = new Redis({
      url: process.env.UPSTASH_REDIS_REST_URL,
      token: process.env.UPSTASH_REDIS_REST_TOKEN,
    });
    console.log('✅ Redis 연결됨 (7일 데이터 보관)');
  } else {
    console.log('⚠️  Redis 미설정 → 인메모리 모드 (서버 재시작 시 초기화)');
  }
} catch (e) {
  console.error('Redis 초기화 실패:', e.message);
}

const TTL = 7 * 24 * 60 * 60; // 7일 (초)

async function rGet(key) {
  if (!redis) return null;
  try { return await redis.get(key); } catch { return null; }
}

async function rSet(key, value) {
  if (!redis) return;
  try { await redis.set(key, value, { ex: TTL }); } catch (e) { console.error('Redis set error:', e.message); }
}

// ==================== IN-MEMORY ROOMS ====================
// rooms: { [roomCode]: { users: { [userId]: { name, budget, items } }, menu, _menuLoaded } }
const rooms = {};
const clientMeta = new Map();

function ensureRoom(roomCode) {
  if (!rooms[roomCode]) {
    rooms[roomCode] = {
      users: {},
      menu: { customItems: [], overrides: {}, hidden: [] },
      _menuLoaded: false,
    };
  }
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

// 빠른 클릭 시 Redis 과호출 방지용 디바운스
const saveTimers = new Map();
function scheduleBasketSave(roomCode, userId) {
  const key = `${roomCode}:${userId}`;
  if (saveTimers.has(key)) clearTimeout(saveTimers.get(key));
  saveTimers.set(key, setTimeout(async () => {
    saveTimers.delete(key);
    const user = rooms[roomCode]?.users[userId];
    if (user) await rSet(`basket:${roomCode}:${user.name}`, user.items);
  }, 800));
}

// ==================== WEBSOCKET ====================
wss.on('connection', (ws) => {
  clientMeta.set(ws, { roomCode: null, userId: null });

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const meta = clientMeta.get(ws);

    try {
      if (msg.type === 'join') {
        const { roomCode, userId, name, budget } = msg;
        meta.roomCode = roomCode;
        meta.userId = userId;
        const room = ensureRoom(roomCode);

        // 첫 접속 시 Redis에서 메뉴 불러오기
        if (!room._menuLoaded) {
          const savedMenu = await rGet(`menu:${roomCode}`);
          if (savedMenu) room.menu = savedMenu;
          room._menuLoaded = true;
        }

        // Redis에서 이 사람의 장바구니 불러오기
        const savedItems = await rGet(`basket:${roomCode}:${name}`);
        room.users[userId] = { name, budget, items: savedItems ?? {} };

        send(ws, { type: 'state', room });
        broadcastToRoom(roomCode, ws, { type: 'userJoined', userId, user: room.users[userId] });

        // TTL 갱신
        if (savedItems) await rSet(`basket:${roomCode}:${name}`, savedItems);
      }

      else if (msg.type === 'updateItems') {
        const { roomCode, userId } = meta;
        if (!roomCode || !userId || !rooms[roomCode]?.users[userId]) return;
        rooms[roomCode].users[userId].items = msg.items;
        broadcastToRoom(roomCode, ws, { type: 'itemsUpdated', userId, items: msg.items });
        scheduleBasketSave(roomCode, userId);
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
        await rSet(`menu:${roomCode}`, msg.menu);
      }

    } catch (err) {
      console.error('Message handler error:', err);
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
