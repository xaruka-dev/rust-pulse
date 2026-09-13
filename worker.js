// worker.js
// Cloudflare Worker — только чтение данных из Upstash Redis, никакого A2S/UDP тут нет
// (Workers не умеют UDP, опрос делает GitHub Actions, см. poll.js).
//
// Секреты (wrangler secret put ...):
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
// Переменная окружения:
//   SERVER_IDS = "server-1,server-2" (список id из servers.json через запятую)

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: cors });
    }

    try {
      if (url.pathname === '/search') {
        // Поиск по ЧАСТИ ника, регистронезависимо.
        const query = (url.searchParams.get('nick') || '').trim().toLowerCase();
        if (!query) return json({ error: 'nick required' }, 400, cors);
        if (query.length < 2) return json({ error: 'nick too short (min 2 chars)' }, 400, cors);

        const allEntries = await redisSmembers(env, 'index:nicknames'); // ["lower|Original", ...]
        const matches = allEntries
          .map((entry) => {
            const sep = entry.indexOf('|');
            return { lower: entry.slice(0, sep), original: entry.slice(sep + 1) };
          })
          .filter((e) => e.lower.includes(query))
          .slice(0, 25);

        if (!matches.length) return json({ query, matches: [] }, 200, cors);

        // Забираем последнюю запись истории + суммарные часы для всех совпадений одним запросом
        const readCommands = matches.flatMap(({ lower }) => [
          ['LRANGE', `nick:${lower}:seen`, '-1', '-1'],
          ['GET', `nick:${lower}:totalSeconds`],
        ]);
        const readResults = await redisPipeline(env, readCommands);

        const results = matches.map(({ lower, original }, i) => {
          const lastRaw = readResults[i * 2] && readResults[i * 2].result;
          const totalSecondsRaw = readResults[i * 2 + 1] && readResults[i * 2 + 1].result;
          const last = (lastRaw && lastRaw[0]) ? JSON.parse(lastRaw[0]) : null;
          return {
            nick: original,
            lastSeen: last ? last.ts : null,
            lastServerId: last ? last.serverId : null,
            totalHours: totalSecondsRaw ? Math.round((Number(totalSecondsRaw) / 3600) * 10) / 10 : 0,
          };
        });

        results.sort((a, b) => (b.lastSeen || 0) - (a.lastSeen || 0));
        return json({ query, matches: results }, 200, cors);
      }

      if (url.pathname === '/history') {
        const nick = url.searchParams.get('nick');
        if (!nick) return json({ error: 'nick required' }, 400, cors);
        const lower = nick.toLowerCase();

        const [historyResult, totalSecondsResult, sessionsResult] = await redisPipeline(env, [
          ['LRANGE', `nick:${lower}:seen`, '-50', '-1'],
          ['GET', `nick:${lower}:totalSeconds`],
          ['GET', `nick:${lower}:sessions`],
        ]);

        const history = (historyResult.result || []).map((item) => JSON.parse(item)).reverse();
        const totalHours = totalSecondsResult.result
          ? Math.round((Number(totalSecondsResult.result) / 3600) * 10) / 10
          : 0;
        const sessions = sessionsResult.result ? Number(sessionsResult.result) : 0;

        return json({ nick, history, totalHours, sessions }, 200, cors);
      }

      if (url.pathname === '/servers') {
        const ids = (env.SERVER_IDS || '').split(',').map((s) => s.trim()).filter(Boolean);
        const snapshots = await Promise.all(
          ids.map(async (id) => {
            const raw = await redisGet(env, `server:${id}:snapshot`);
            const parsed = raw ? JSON.parse(raw) : { ts: null, players: [] };
            return { id, ...parsed };
          })
        );
        return json(snapshots, 200, cors);
      }

      return json({ error: 'not found' }, 404, cors);
    } catch (err) {
      return json({ error: err.message }, 500, cors);
    }
  },
};

async function redisGet(env, key) {
  const res = await fetch(`${env.UPSTASH_REDIS_REST_URL}/get/${encodeURIComponent(key)}`, {
    headers: { Authorization: `Bearer ${env.UPSTASH_REDIS_REST_TOKEN}` },
  });
  const data = await res.json();
  return data.result;
}

async function redisSmembers(env, key) {
  const res = await fetch(`${env.UPSTASH_REDIS_REST_URL}/smembers/${encodeURIComponent(key)}`, {
    headers: { Authorization: `Bearer ${env.UPSTASH_REDIS_REST_TOKEN}` },
  });
  const data = await res.json();
  return data.result || [];
}

// Несколько команд в одном HTTP-запросе. commands: [["LRANGE","key","-1","-1"], ["GET","key2"], ...]
async function redisPipeline(env, commands) {
  if (!commands.length) return [];
  const res = await fetch(`${env.UPSTASH_REDIS_REST_URL}/pipeline`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${env.UPSTASH_REDIS_REST_TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(commands),
  });
  if (!res.ok) throw new Error(`Redis pipeline failed: ${res.status}`);
  return res.json();
}

function json(obj, status, headers) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { 'Content-Type': 'application/json', ...headers },
  });
}
