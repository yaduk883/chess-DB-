// Chess relay server: rooms, turn checking, time controls, reconnect by token.
const http = require('http');
const { WebSocketServer } = require('ws');

const srv = http.createServer((q, r) => { r.writeHead(200); r.end('chess server ok'); });
const wss = new WebSocketServer({ server: srv, maxPayload: 2048 });
const rooms = new Map();
const ABC = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const send = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));
const roleOf = (rm, tok) => tok === rm.wtok ? 'w' : tok === rm.btok ? 'b' : 'x';
const turnOf = rm => rm.moves.length % 2 ? 'b' : 'w';
const running = rm => rm.tc > 0 && rm.moves.length > 0 && !rm.flag;   // clocks start after White's first move
function push(rm) {
  const t = { w: rm.wt, b: rm.bt }, run = running(rm);
  if (run) t[turnOf(rm)] = Math.max(0, t[turnOf(rm)] - (Date.now() - rm.last));
  rm.clients.forEach(c => send(c, { t: 'state', code: rm.code, role: roleOf(rm, c.tok), moves: rm.moves, opp: !!rm.btok, tc: rm.tc, wt: t.w, bt: t.b, run, flag: rm.flag, end: rm.end, offer: rm.offer, rmq: rm.rmq, g: rm.g }));
}
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
        const tc = [180, 300, 600].includes(m.tc) ? m.tc : 0;
        rm = { code: newCode(), moves: [], wtok: m.tok, btok: null, clients: new Set(), t: Date.now(), tc, wt: tc * 1000, bt: tc * 1000, last: 0, flag: null, end: null, offer: null, rmq: null, g: 0 };
        rooms.set(rm.code, rm);
      } else {
        rm = rooms.get(String(m.code || '').toUpperCase());
        if (!rm) return send(ws, { t: 'err', m: 'No game with that code' });
        if (!rm.btok && m.tok !== rm.wtok) rm.btok = m.tok;
      }
      rm.clients.add(ws); rm.t = Date.now(); push(rm);
    } else if (m.t === 'move' && rm) {
      const turn = turnOf(rm);
      if (rm.flag || rm.end || roleOf(rm, ws.tok) !== turn || !/^\d{1,2}-\d{1,2}(-[qrbn])?$/.test(m.mv)) return;
      const now = Date.now();
      if (running(rm)) {
        rm[turn + 't'] -= now - rm.last;
        if (rm[turn + 't'] <= 0) { rm[turn + 't'] = 0; rm.flag = turn; rm.t = now; return push(rm); }
      }
      rm.moves.push(m.mv); rm.offer = null; rm.last = now; rm.t = now; push(rm);
    } else if (rm && ['resign', 'draw', 'rematch'].includes(m.t)) {
      const role = roleOf(rm, ws.tok);
      if (role === 'x') return;
      if (m.t === 'resign' && !rm.end) rm.end = (role === 'w' ? 'White' : 'Black') + ' resigned - ' + (role === 'w' ? 'Black' : 'White') + ' wins';
      else if (m.t === 'draw' && !rm.end) { if (rm.offer && rm.offer !== role) rm.end = 'Draw agreed'; else rm.offer = role; }
      else if (m.t === 'rematch' && rm.btok) {
        if (rm.rmq && rm.rmq !== role) { [rm.wtok, rm.btok] = [rm.btok, rm.wtok]; Object.assign(rm, { moves: [], wt: rm.tc * 1000, bt: rm.tc * 1000, last: 0, flag: null, end: null, offer: null, rmq: null, g: rm.g + 1 }); }
        else rm.rmq = role;
      }
      rm.t = Date.now(); push(rm);
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
