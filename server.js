const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static(__dirname));

// ==================== STORAGE (Redis 우선, 없으면 로컬 JSON 파일) ====================
let redis = null;
const LOCAL_DATA_FILE = path.join(__dirname, '.room-data.json');

try {
  if (process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN) {
    const { Redis } = require('@upstash/redis');
    redis = new Redis({
      url: process.env.UPSTASH_REDIS_REST_URL,
      token: process.env.UPSTASH_REDIS_REST_TOKEN,
    });
    console.log('✅ Redis 연결됨 (7일 데이터 보관)');
  } else {
    console.log('⚠️  Redis 미설정 → 로컬 파일 모드 (.room-data.json)');
  }
} catch (e) {
  console.error('Redis 초기화 실패:', e.message);
}

const TTL = 7 * 24 * 60 * 60; // 7일

// 로컬 파일 읽기/쓰기
function fileGet(key) {
  try {
    const data = JSON.parse(fs.readFileSync(LOCAL_DATA_FILE, 'utf8'));
    const entry = data[key];
    if (!entry) return null;
    // TTL 체크 (7일 초과 시 무효)
    if (Date.now() - entry.ts > TTL * 1000) return null;
    return entry.value;
  } catch { return null; }
}

function fileSet(key, value) {
  try {
    let data = {};
    try { data = JSON.parse(fs.readFileSync(LOCAL_DATA_FILE, 'utf8')); } catch {}
    data[key] = { value, ts: Date.now() };
    fs.writeFileSync(LOCAL_DATA_FILE, JSON.stringify(data));
  } catch (e) { console.error('파일 저장 오류:', e.message); }
}

async function rGet(key) {
  if (redis) {
    try { return await redis.get(key); } catch { return null; }
  }
  return fileGet(key);
}

async function rSet(key, value) {
  if (redis) {
    try { await redis.set(key, value, { ex: TTL }); } catch (e) { console.error('Redis set error:', e.message); }
    return;
  }
  fileSet(key, value);
}

// ==================== IN-MEMORY ROOMS ====================
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

// 빠른 클릭 시 과호출 방지 디바운스
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

async function flushBasketSave(roomCode, userId) {
  const key = `${roomCode}:${userId}`;
  if (saveTimers.has(key)) {
    clearTimeout(saveTimers.get(key));
    saveTimers.delete(key);
  }
  const user = rooms[roomCode]?.users[userId];
  if (user) await rSet(`basket:${roomCode}:${user.name}`, user.items);
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

        // 첫 접속 시 저장된 메뉴 불러오기
        if (!room._menuLoaded) {
          const savedMenu = await rGet(`menu:${roomCode}`);
          if (savedMenu) room.menu = savedMenu;
          room._menuLoaded = true;
        }

        // 저장된 장바구니 불러오기
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

  ws.on('close', async () => {
    try {
      const { roomCode, userId } = clientMeta.get(ws) || {};
      if (roomCode && userId && rooms[roomCode]) {
        // 디바운스 대기 중인 저장이 있으면 즉시 실행
        await flushBasketSave(roomCode, userId);
        delete rooms[roomCode].users[userId];
        broadcastToRoom(roomCode, ws, { type: 'userLeft', userId });
        if (Object.keys(rooms[roomCode].users).length === 0) delete rooms[roomCode];
      }
      clientMeta.delete(ws);
    } catch (err) {
      console.error('Close handler error:', err);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🍞 성심당 서버 실행 중: http://localhost:${PORT}`);
});
