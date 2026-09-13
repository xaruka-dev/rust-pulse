// worker.js
// Cloudflare Worker — только чтение данных из Upstash Redis, никакого A2S/UDP тут нет
// (Workers не умеют UDP, поэтому опрос теперь делает GitHub Actions, см. poll.js).
//
// Секреты, которые нужно задать в Cloudflare (wrangler secret put ...):
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
// Переменная окружения (обычная, не секрет):
//   SERVER_IDS = "myserver,anotherserver"  (список id из servers.json через запятую)

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
        // Возвращает список подходящих ников с кратким summary по каждому,
        // а не сразу полную историю — если совпадений несколько, фронт даёт выбрать.
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
          .slice(0, 25); // не даём одному запросу утащить сотни совпадений

        const results = await Promise.all(
          matches.map(async ({ lower, original }) => {
            const key = `nick:${lower}:seen`;
            const lastEntries = await redisLrange(env, key, -1, -1);
            const last = lastEntries[0] || null;
            return { nick: original, lastSeen: last ? last.ts : null, lastServerId: last ? last.serverId : null };
          })
        );

        // Сначала недавно виденные
        results.sort((a, b) => (b.lastSeen || 0) - (a.lastSeen || 0));

        return json({ query, matches: results }, 200, cors);
      }

      if (url.pathname === '/history') {
        // Полная история конкретного (точного) ника
        const nick = url.searchParams.get('nick');
        if (!nick) return json({ error: 'nick required' }, 400, cors);

        const key = `nick:${nick.toLowerCase()}:seen`;
        const history = await redisLrange(env, key, -50, -1);
        return json({ nick, history: history.reverse() }, 200, cors);
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

async function redisLrange(env, key, start, stop) {
  const res = await fetch(
    `${env.UPSTASH_REDIS_REST_URL}/lrange/${encodeURIComponent(key)}/${start}/${stop}`,
    { headers: { Authorization: `Bearer ${env.UPSTASH_REDIS_REST_TOKEN}` } }
  );
  const data = await res.json();
  return (data.result || []).map((item) => JSON.parse(item));
}

function json(obj, status, headers) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { 'Content-Type': 'application/json', ...headers },
  });
}
