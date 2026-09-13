// poll.js
// Запускается из GitHub Actions раз в ~5 минут (см. .github/workflows/poll.yml).
// Опрашивает сервера из servers.json по A2S и складывает результат в Upstash Redis.
//
// Рассчитан на 100+ серверов:
//   - опрос идёт пачками параллельно (POLL_CONCURRENCY);
//   - в историю ников пишутся только НОВЫЕ заходы, а не весь онлайн каждый раз;
//   - для каждого ника считаем суммарные часы на сервере: при заходе запоминаем
//     время старта сессии, при выходе — прибавляем длительность к общему счётчику.
//
// Ограничение точности: сессия и часы считаются с точностью до интервала опроса
// (~5 минут). Если между двумя опросами игрок зашёл и вышел — мы это не увидим.

const fs = require('fs');
const path = require('path');
const { queryA2SPlayers } = require('./a2s');

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const CONCURRENCY = Number(process.env.POLL_CONCURRENCY || 20);

if (!UPSTASH_URL || !UPSTASH_TOKEN) {
  console.error('Не заданы UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN (GitHub Secrets).');
  process.exit(1);
}

// Пакетный вызов Redis: несколько команд одним HTTP-запросом.
// Формат каждой команды — массив ["SET", "key", "value"] и т.п.
async function redisPipeline(commands) {
  if (!commands.length) return [];
  const res = await fetch(`${UPSTASH_URL}/pipeline`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(commands),
  });
  if (!res.ok) {
    throw new Error(`Redis pipeline failed: ${res.status} ${await res.text()}`);
  }
  return res.json(); // [{ result: ... }, { result: ... }, ...]
}

// Простой ограничитель параллелизма
async function runWithConcurrency(items, limit, worker) {
  const results = new Array(items.length);
  let next = 0;

  async function runner() {
    while (next < items.length) {
      const i = next++;
      results[i] = await worker(items[i], i).catch((err) => ({ __error: err }));
    }
  }

  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, runner));
  return results;
}

async function pollServer(server) {
  const { id, host, port } = server;
  const now = Date.now();

  let players;
  try {
    players = await queryA2SPlayers(host, port);
  } catch (err) {
    console.error(`[FAIL] ${id} (${host}:${port}): ${err.message}`);
    return { id, ok: false, commands: [] };
  }

  const names = [...new Set(players.map((p) => p.name).filter(Boolean))];
  const currentLower = new Set(names.map((n) => n.toLowerCase()));
  const nameByLower = new Map(names.map((n) => [n.toLowerCase(), n]));

  // Читаем прошлый снимок сервера одним запросом
  const [prevSnapshotResult] = await redisPipeline([
    ['GET', `server:${id}:snapshot`],
  ]);
  let previousNames = [];
  try {
    if (prevSnapshotResult && prevSnapshotResult.result) {
      previousNames = JSON.parse(prevSnapshotResult.result).players || [];
    }
  } catch (_) {
    // прошлый снимок битый или отсутствует — считаем, что все текущие "зашли только что"
  }
  const previousLower = new Set(previousNames.map((n) => n.toLowerCase()));

  const joined = names.filter((n) => !previousLower.has(n.toLowerCase()));
  const leftLower = [...previousLower].filter((n) => !currentLower.has(n));

  // Для тех, кто вышел, нужно узнать, когда началась их сессия, чтобы посчитать часы
  let sessionStarts = [];
  if (leftLower.length) {
    const readCommands = leftLower.map((lower) => ['GET', `session:${id}:${lower}`]);
    sessionStarts = await redisPipeline(readCommands);
  }

  const commands = [
    ['SET', `server:${id}:snapshot`, JSON.stringify({ ts: now, players: names }), 'EX', '900'],
  ];

  // Новые заходы: запоминаем начало сессии + пишем в историю + в индекс ников
  for (const name of joined) {
    const lower = name.toLowerCase();
    commands.push(['SET', `session:${id}:${lower}`, String(now), 'EX', '86400']);
    commands.push(['RPUSH', `nick:${lower}:seen`, JSON.stringify({ ts: now, serverId: id, name })]);
    commands.push(['LTRIM', `nick:${lower}:seen`, '-200', '-1']);
    commands.push(['SADD', 'index:nicknames', `${lower}|${name}`]);
  }

  // Вышедшие: считаем длительность сессии и прибавляем к общему счётчику часов
  leftLower.forEach((lower, i) => {
    const startRaw = sessionStarts[i] && sessionStarts[i].result;
    commands.push(['DEL', `session:${id}:${lower}`]);
    if (startRaw) {
      const start = Number(startRaw);
      const durationSec = Math.max(0, Math.round((now - start) / 1000));
      if (durationSec > 0) {
        commands.push(['INCRBY', `nick:${lower}:totalSeconds`, String(durationSec)]);
      }
      commands.push(['INCR', `nick:${lower}:sessions`]);
    }
  });

  console.log(
    `[OK] ${id} (${host}:${port}) — онлайн: ${names.length}, зашли: ${joined.length}, вышли: ${leftLower.length}`
  );
  return { id, ok: true, commands };
}

async function main() {
  const serversPath = path.join(__dirname, 'servers.json');
  const servers = JSON.parse(fs.readFileSync(serversPath, 'utf8'));

  await redisPipeline([['SET', 'meta:server_ids', JSON.stringify(servers.map((s) => s.id))]]);

  const results = await runWithConcurrency(servers, CONCURRENCY, pollServer);

  const allCommands = [];
  let failed = 0;
  for (const r of results) {
    if (r.__error || !r.ok) {
      failed++;
      continue;
    }
    allCommands.push(...r.commands);
  }

  const CHUNK = 500;
  for (let i = 0; i < allCommands.length; i += CHUNK) {
    await redisPipeline(allCommands.slice(i, i + CHUNK));
  }

  console.log(
    `Готово: ${servers.length - failed}/${servers.length} серверов опрошено успешно, команд в Redis: ${allCommands.length}`
  );
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
