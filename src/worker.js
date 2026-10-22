import { DurableObject } from 'cloudflare:workers';

// 联机房间：座位 0 是房主，1-3 是加入方。房主的消息广播给所有加入方，
// 加入方的消息只转给房主，并带上发送者的座位号。
// 聊天（"C|频道|内容"）由房间直接分发：盟友频道只发给同盟玩家，不经过房主。
export class Room extends DurableObject {
  slotOf(ws) { const a = ws.deserializeAttachment(); return a ? a.slot : -1; }
  peers(except) { return this.ctx.getWebSockets().filter(s => s !== except && this.slotOf(s) >= 0); }
  safeSend(ws, msg) { try { ws.send(msg); } catch (e) {} }
  roster(except) {
    const peers = this.peers(except), msg = JSON.stringify({ t: 'roster', slots: peers.map(s => this.slotOf(s)).sort() });
    for (const s of peers) this.safeSend(s, msg);
  }

  async fetch(req) {
    const as = new URL(req.url).searchParams.get('as') === 'host' ? 'host' : 'guest';
    const peers = this.peers();
    let slot = 0, refuse = null;
    if (as === 'host') { if (peers.length) refuse = 'taken'; else await this.ctx.storage.deleteAll(); }
    else if (!peers.some(s => this.slotOf(s) === 0)) refuse = 'empty';
    else if (await this.ctx.storage.get('locked')) refuse = 'started';
    else {
      const used = new Set(peers.map(s => this.slotOf(s)));
      slot = [1, 2, 3].find(i => !used.has(i));
      if (slot === undefined) refuse = 'full';
    }
    const pair = new WebSocketPair(), client = pair[0], server = pair[1];
    this.ctx.acceptWebSocket(server);
    if (refuse) { server.send(JSON.stringify({ t: refuse })); server.close(1000, refuse); }
    else { server.serializeAttachment({ slot }); server.send(JSON.stringify({ t: 'role', slot })); this.roster(); }
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, msg) {
    const slot = this.slotOf(ws);
    if (slot < 0 || typeof msg !== 'string') return;
    if (msg.startsWith('C|')) {
      const i = msg.indexOf('|', 2); if (i < 0) return;
      const text = msg.slice(i + 1).trim().slice(0, 120); if (!text) return;
      const ally = msg.slice(2, i) === 'team' ? await this.ctx.storage.get('ally') : null;
      const out = JSON.stringify({ t: 'c', srv: 1, who: slot, text, ch: ally ? 'team' : 'all' });
      for (const s of this.peers()) if (!ally || ally[this.slotOf(s)] === ally[slot]) this.safeSend(s, out);
      return;
    }
    if (slot === 0) {
      if (msg.startsWith('LOCK')) {
        let ally = null; try { ally = JSON.parse(msg.slice(4) || 'null'); } catch (e) {}
        if (!Array.isArray(ally) || ally.length !== 4 || !ally.every(Number.isInteger)) ally = null;
        await this.ctx.storage.put({ locked: true, ally });
        return;
      }
      for (const s of this.peers(ws)) this.safeSend(s, msg);
    } else {
      const host = this.peers(ws).find(s => this.slotOf(s) === 0);
      if (!host) return;
      // 先解析再重新序列化，防止加入方在原始字符串里拼出第二个 "from" 冒充别的座位
      let m; try { m = JSON.parse(msg); } catch (e) { return; }
      if (!m || typeof m !== 'object' || Array.isArray(m)) return;
      this.safeSend(host, JSON.stringify({ from: slot, m }));
    }
  }

  async webSocketClose(ws) { await this.bye(ws); }
  async webSocketError(ws) { await this.bye(ws); }

  async bye(ws) {
    const slot = this.slotOf(ws);
    try { ws.close(); } catch (e) {}
    if (slot < 0) return;
    const others = this.peers(ws), left = JSON.stringify({ t: 'left', slot });
    for (const s of others) this.safeSend(s, left);
    if (slot === 0) { await this.ctx.storage.deleteAll(); for (const s of others) { try { s.close(1000, 'host left'); } catch (e) {} } }
    else this.roster(ws);
  }
}

export default {
  async fetch(req, env) {
    const m = new URL(req.url).pathname.match(/^\/rts\/ws\/(\d{4})$/);
    if (m) {
      if (req.headers.get('Upgrade') !== 'websocket') return new Response('expected websocket', { status: 426 });
      return env.ROOMS.get(env.ROOMS.idFromName(m[1])).fetch(req);
    }
    return env.ASSETS.fetch(req);
  },
};
