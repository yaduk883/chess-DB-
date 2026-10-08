// Minimal chess relay server: rooms, turn checking, reconnect by token.
const http = require('http');
const { WebSocketServer } = require('ws');

const srv = http.createServer((q, r) => { r.writeHead(200); r.end('chess server ok'); });
const wss = new WebSocketServer({ server: srv, maxPayload: 2048 });
const rooms = new Map();
const ABC = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const send = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));
const roleOf = (rm, tok) => tok === rm.wtok ? 'w' : tok === rm.btok ? 'b' : 'x';
const push = rm => rm.clients.forEach(c => send(c, { t: 'state', code: rm.code, role: roleOf(rm, c.tok), moves: rm.moves, opp: !!rm.btok }));
const newCode = () => { let c; do { c = Array.from({ length: 6 }, () => ABC[Math.random() * ABC.length | 0]).join(''); } while (rooms.has(c)); return c; };

wss.on('connection', ws => {
  let rm = null;
  ws.isAlive = true;
  ws.on('pong', () => ws.isAlive = true);
  ws.on('message', d => {
    let m; try { m = JSON.parse(d); } catch { return; }
    if (m.t === 'create' || m.t === 'join') {
      if (typeof m.tok !== 'string' || m.tok.length > 40) return;
      ws.tok = m.tok;
      if (m.t === 'create') {
        rm = { code: newCode(), moves: [], wtok: m.tok, btok: null, clients: new Set(), t: Date.now() };
        rooms.set(rm.code, rm);
      } else {
        rm = rooms.get(String(m.code || '').toUpperCase());
        if (!rm) return send(ws, { t: 'err', m: 'No game with that code' });
        if (!rm.btok && m.tok !== rm.wtok) rm.btok = m.tok;
      }
      rm.clients.add(ws); rm.t = Date.now(); push(rm);
    } else if (m.t === 'move' && rm) {
      const turn = rm.moves.length % 2 ? 'b' : 'w';
      if (roleOf(rm, ws.tok) !== turn || !/^\d{1,2}-\d{1,2}(-[qrbn])?$/.test(m.mv)) return;
      rm.moves.push(m.mv); rm.t = Date.now(); push(rm);
    }
  });
  ws.on('close', () => { if (rm) rm.clients.delete(ws); });
});

setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping(); });
  const now = Date.now();
  for (const [c, rm] of rooms) if (now - rm.t > (rm.clients.size ? 24 : 3) * 3600e3) rooms.delete(c);
}, 30000);

srv.listen(process.env.PORT || 8080, () => console.log('chess server running'));
