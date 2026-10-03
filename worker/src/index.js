// Rider Call - signaling (D1 long-poll) + WebSocket media relay (Durable Object) Worker
// Bindings:
//   DB                  -> D1 database "rider-call-signal" (tables: users, msgs)
//   RELAY               -> Durable Object namespace (class Relay), one instance per call
//   TURN_KEY_ID         -> (variable, optional) Cloudflare Realtime TURN key ID
//   TURN_KEY_API_TOKEN  -> (secret, optional)   Cloudflare Realtime TURN key API token
//   APP_KEY             -> (secret, optional) shared key; app must send header X-App-Key

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,X-App-Key",
};
const json = (data, status = 200) =>
  new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...CORS } });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const validId = (s) => typeof s === "string" && /^[A-Za-z0-9_\-]{3,64}$/.test(s);
const ALLOWED_TYPES = new Set(["call", "offer", "answer", "ice", "accept", "reject", "busy", "hangup", "video-on", "video-off", "ping"]);

export default {
  async fetch(req, env) {
    if (req.method === "OPTIONS") return new Response(null, { headers: CORS });
    const url = new URL(req.url);

    try {
      // WebSocket media relay: /relay?room=<callId>&me=<userId>
      if (url.pathname === "/relay") {
        const room = url.searchParams.get("room");
        const me = url.searchParams.get("me");
        if (!validId(room) || !validId(me)) return json({ error: "bad room" }, 400);
        if (req.headers.get("Upgrade") !== "websocket") return json({ error: "expected websocket" }, 426);
        return env.RELAY.get(env.RELAY.idFromName(room)).fetch(req);
      }

      if (env.APP_KEY && req.headers.get("X-App-Key") !== env.APP_KEY) return json({ error: "unauthorized" }, 401);

      // Health check
      if (url.pathname === "/" || url.pathname === "/health") {
        return json({ ok: true, turn: !!(env.TURN_KEY_ID && env.TURN_KEY_API_TOKEN), time: Date.now() });
      }

      // ICE servers (STUN always, TURN when configured)
      if (url.pathname === "/ice") {
        const stun = { urls: ["stun:stun.cloudflare.com:3478", "stun:stun.cloudflare.com:53", "stun:stun.l.google.com:19302"] };
        if (!env.TURN_KEY_ID || !env.TURN_KEY_API_TOKEN) return json({ iceServers: [stun], turn: false });
        const r = await fetch(
          `https://rtc.live.cloudflare.com/v1/turn/keys/${env.TURN_KEY_ID}/credentials/generate-ice-servers`,
          {
            method: "POST",
            headers: { Authorization: `Bearer ${env.TURN_KEY_API_TOKEN}`, "Content-Type": "application/json" },
            body: JSON.stringify({ ttl: 86400 }),
          }
        );
        if (!r.ok) return json({ iceServers: [stun], turn: false, error: `turn ${r.status}` });
        const data = await r.json();
        return json({ iceServers: data.iceServers, turn: true });
      }

      // Register / heartbeat: POST {id, name}
      if (url.pathname === "/register" && req.method === "POST") {
        const { id, name } = await req.json();
        if (!validId(id)) return json({ error: "bad id" }, 400);
        await env.DB.prepare(
          "INSERT INTO users (id, name, last_seen) VALUES (?1, ?2, ?3) ON CONFLICT(id) DO UPDATE SET name = ?2, last_seen = ?3"
        ).bind(id, String(name || "").slice(0, 64), Date.now()).run();
        return json({ ok: true });
      }

      // Is a user online? GET /status?id=X
      if (url.pathname === "/status") {
        const id = url.searchParams.get("id");
        if (!validId(id)) return json({ error: "bad id" }, 400);
        const u = await env.DB.prepare("SELECT name, last_seen FROM users WHERE id = ?1").bind(id).first();
        return json({ exists: !!u, name: u?.name || null, online: !!u && Date.now() - u.last_seen < 60000 });
      }

      // Send signaling message: POST {from, to, type, payload}
      if (url.pathname === "/send" && req.method === "POST") {
        const { from, to, type, payload } = await req.json();
        if (!validId(from) || !validId(to) || !ALLOWED_TYPES.has(type)) return json({ error: "bad message" }, 400);
        const p = payload === undefined ? null : JSON.stringify(payload);
        if (p && p.length > 20000) return json({ error: "payload too large" }, 413);
        const r = await env.DB.prepare("INSERT INTO msgs (to_id, from_id, type, payload, ts) VALUES (?1, ?2, ?3, ?4, ?5)")
          .bind(to, from, type, p, Date.now()).run();
        return json({ ok: true, id: r.meta.last_row_id });
      }

      // Long-poll inbox: GET /poll?me=X&since=N  (waits up to ~20s)
      if (url.pathname === "/poll") {
        const me = url.searchParams.get("me");
        const since = parseInt(url.searchParams.get("since") || "0", 10) || 0;
        if (!validId(me)) return json({ error: "bad id" }, 400);
        const now = Date.now();
        await env.DB.prepare("UPDATE users SET last_seen = ?2 WHERE id = ?1").bind(me, now).run();
        // housekeeping: delete delivered + stale messages
        await env.DB.prepare("DELETE FROM msgs WHERE (to_id = ?1 AND id <= ?2) OR ts < ?3").bind(me, since, now - 120000).run();

        const deadline = now + 20000;
        let wait = 300;
        while (true) {
          const { results } = await env.DB.prepare(
            "SELECT id, from_id AS 'from', type, payload, ts FROM msgs WHERE to_id = ?1 AND id > ?2 ORDER BY id LIMIT 100"
          ).bind(me, since).all();
          if (results.length || Date.now() > deadline) {
            return json({
              messages: results.map((m) => ({ ...m, payload: m.payload ? JSON.parse(m.payload) : null })),
            });
          }
          await sleep(wait);
          wait = Math.min(wait + 150, 1000);
        }
      }

      return json({ error: "not found" }, 404);
    } catch (e) {
      return json({ error: String(e && e.message || e) }, 500);
    }
  },
};

// One Durable Object per call. Forwards every message from one participant to the other(s).
export class Relay {
  constructor(ctx, env) {
    this.ctx = ctx;
  }

  async fetch(req) {
    const me = new URL(req.url).searchParams.get("me");
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    // a reconnect from the same user replaces the old socket
    for (const ws of this.ctx.getWebSockets(me)) {
      try { ws.close(1000, "replaced"); } catch (e) {}
    }
    this.ctx.acceptWebSocket(server, [me]);
    const others = this.ctx.getWebSockets().filter((ws) => ws !== server && this.ctx.getTags(ws)[0] !== me);
    for (const ws of others) {
      try { ws.send(JSON.stringify({ t: "peer-joined", id: me })); } catch (e) {}
    }
    if (others.length) server.send(JSON.stringify({ t: "peer-joined", id: this.ctx.getTags(others[0])[0] }));
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(ws, msg) {
    for (const o of this.ctx.getWebSockets()) {
      if (o !== ws) {
        try { o.send(msg); } catch (e) {}
      }
    }
  }

  webSocketClose(ws, code, reason) {
    const me = this.ctx.getTags(ws)[0];
    for (const o of this.ctx.getWebSockets()) {
      if (o !== ws && this.ctx.getTags(o)[0] !== me) {
        try { o.send(JSON.stringify({ t: "peer-left", id: me, reason: String(reason || "") })); } catch (e) {}
      }
    }
    try { ws.close(code, reason); } catch (e) {}
  }

  webSocketError(ws) {
    this.webSocketClose(ws, 1011, "error");
  }
}
