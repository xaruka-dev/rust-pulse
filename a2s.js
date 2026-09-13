// a2s.js
// Реализация A2S_PLAYER запроса (родной протокол опроса Source-серверов,
// его же под капотом использует и сам BattleMetrics, и любой браузер серверов).
// Не отдаёт SteamID — только ник, score и время на сервере.

const dgram = require('dgram');

function buildA2SPlayerRequest(challenge) {
  // FF FF FF FF 55 <challenge int32>
  const buf = Buffer.alloc(9);
  buf.writeInt32LE(-1, 0);
  buf.writeUInt8(0x55, 4);
  buf.writeInt32LE(challenge, 5);
  return buf;
}

function parseA2SPlayerResponse(buf) {
  // FF FF FF FF 44 <numPlayers> [<index><name>\0<score int32><duration float>]...
  let offset = 5;
  const numPlayers = buf.readUInt8(offset);
  offset += 1;

  const players = [];
  for (let i = 0; i < numPlayers; i++) {
    offset += 1; // index байт, не используется
    const nameEnd = buf.indexOf(0x00, offset);
    const name = buf.toString('utf8', offset, nameEnd);
    offset = nameEnd + 1;
    const score = buf.readInt32LE(offset);
    offset += 4;
    const duration = buf.readFloatLE(offset);
    offset += 4;
    if (name) players.push({ name, score, duration });
  }
  return players;
}

/**
 * Запрашивает список игроков на сервере по адресу host:port.
 * Возвращает Promise<Array<{name, score, duration}>>.
 */
function queryA2SPlayers(host, port, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const socket = dgram.createSocket('udp4');
    let timeout;
    let settled = false;

    const cleanup = () => {
      clearTimeout(timeout);
      try { socket.close(); } catch (_) {}
    };

    const finish = (fn, arg) => {
      if (settled) return;
      settled = true;
      cleanup();
      fn(arg);
    };

    timeout = setTimeout(() => {
      finish(reject, new Error(`Timeout querying ${host}:${port}`));
    }, timeoutMs);

    socket.on('error', (err) => finish(reject, err));

    socket.on('message', (msg) => {
      if (msg.length < 5) return;
      const header = msg.readUInt8(4);

      if (header === 0x41) {
        // S2C_CHALLENGE — пересылаем запрос с полученным challenge-номером
        const challenge = msg.readInt32LE(5);
        socket.send(buildA2SPlayerRequest(challenge), port, host);
        return;
      }

      if (header === 0x44) {
        // S2A_PLAYER — сам список игроков
        try {
          finish(resolve, parseA2SPlayerResponse(msg));
        } catch (err) {
          finish(reject, err);
        }
      }
    });

    // Начальный запрос с challenge = -1 (некоторые сервера отвечают сразу списком)
    socket.send(buildA2SPlayerRequest(-1), port, host);
  });
}

module.exports = { queryA2SPlayers };
