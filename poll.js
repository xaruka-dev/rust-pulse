// poll.js
// Запускается из GitHub Actions раз в ~5 минут (см. .github/workflows/poll.yml).
// Опрашивает все сервера из servers.json по A2S и складывает результат
// в Upstash Redis (бесплатный REST-доступ к Redis, без своего сервера).

const fs = require('fs');
const path = require('path');
const { queryA2SPlayers } = require('./a2s');

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

if (!UPSTASH_URL || !UPSTASH_TOKEN) {
  console.error('Не заданы UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN (GitHub Secrets).');
  process.exit(1);
}

async function redisCall(pathSegments) {
  const url = `${UPSTASH_URL}/${pathSegments.map(encodeURIComponent).join('/')}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
  });
  if (!res.ok) {
    throw new Error(`Redis ${pathSegments[0]} failed: ${res.status} ${await res.text()}`);
  }
  return res.json();
}

const redisSet = (key, value, exSeconds) =>
  redisCall(exSeconds ? ['set', key, value, 'EX', String(exSeconds)] : ['set', key, value]);

const redisRpush = (key, value) => redisCall(['rpush', key, value]);

// Обрезаем историю ника, чтобы список не рос бесконечно (оставляем последние 500 записей)
const redisLtrim = (key, start, stop) => redisCall(['ltrim', key, String(start), String(stop)]);

// Индекс всех когда-либо виденных ников — нужен для поиска по части ника
// (Redis сам по себе не умеет искать по подстроке, поэтому держим список
// всех ников отдельно и фильтруем его на стороне Worker'а).
const redisSadd = (key, value) => redisCall(['sadd', key, value]);

async function main() {
  const serversPath = path.join(__dirname, 'servers.json');
  const servers = JSON.parse(fs.readFileSync(serversPath, 'utf8'));
  const now = Date.now();

  const knownServerIds = servers.map((s) => s.id);
  await redisSet('meta:server_ids', JSON.stringify(knownServerIds));

  for (const server of servers) {
    const { id, host, port } = server;
    try {
      const players = await queryA2SPlayers(host, port);
      const names = players.map((p) => p.name).filter(Boolean);

      // Снимок текущего онлайна сервера (TTL 15 минут — если опрос перестанет ходить,
      // фронт увидит, что данные устарели, а не покажет вечно старый список)
      await redisSet(`server:${id}:snapshot`, JSON.stringify({ ts: now, players: names }), 900);

      // История "видели ник на сервере в такое-то время" — для поиска игрока по нику
      for (const name of names) {
        const key = `nick:${name.toLowerCase()}:seen`;
        await redisRpush(key, JSON.stringify({ ts: now, serverId: id, name }));
        await redisLtrim(key, -500, -1);
        // Кладём в общий индекс ников в исходном регистре, ключ поиска — lowercase
        await redisSadd('index:nicknames', `${name.toLowerCase()}|${name}`);
      }

      console.log(`[OK] ${id} (${host}:${port}) — игроков: ${names.length}`);
    } catch (err) {
      console.error(`[FAIL] ${id} (${host}:${port}): ${err.message}`);
    }
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
