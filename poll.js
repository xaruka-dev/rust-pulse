const fs = require('fs');

const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

async function run() {
  if (!REDIS_URL || !REDIS_TOKEN) {
    console.error("ОШИБКА: Не заданы секреты UPSTASH_REDIS_REST_URL или UPSTASH_REDIS_REST_TOKEN!");
    process.exit(1);
  }

  // 1. Читаем ID серверов из JSON
  const rawServers = JSON.parse(fs.readFileSync('./servers.json', 'utf8'));
  const ids = rawServers.map(s => s.id).filter(Boolean);

  if (ids.length === 0) {
    console.log("В servers.json нет ID для запроса.");
    return;
  }

  console.log(`Запрашиваем данные для ${ids.length} серверов с BattleMetrics...`);

  try {
    // 2. Делаем BATCH-запрос за 1 раз (до 100 серверов в одном URL)
    const url = `https://api.battlemetrics.com/servers?filter[ids]=${ids.join(',')}&page[size]=100`;
    const res = await fetch(url);
    const body = await res.json();

    if (!body.data) {
      console.error("Ошибка ответа API:", body);
      process.exit(1);
    }

    let successCount = 0;

    // 3. Сохраняем каждый сервер в Redis
    for (const server of body.data) {
      const attr = server.attributes;
      const serverPayload = {
        id: server.id,
        name: attr.name,
        ip: attr.ip,
        port: attr.port,
        players: attr.players,
        maxPlayers: attr.maxPlayers,
        status: attr.status,
        details: attr.details,
        updatedAt: new Date().toISOString()
      };

      // Сохраняем по ключу server:<id>
      await fetch(`${REDIS_URL}/set/server:${server.id}`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${REDIS_TOKEN}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(JSON.stringify(serverPayload))
      });

      console.log(`[OK] ${attr.name} — игроков: ${attr.players}/${attr.maxPlayers}`);
      successCount++;
    }

    console.log(`Готово: ${successCount}/${ids.length} серверов успешно обновлено в Redis.`);
  } catch (err) {
    console.error("Ошибка выполнения скрипта:", err);
    process.exit(1);
  }
}

run();
