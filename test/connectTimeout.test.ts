// Falcon handshake deadline: WebSocket-hostile infra that *blackholes* the
// upgrade — completing the WS handshake (101) but never delivering `welcome` —
// must not hang connect() forever. It must reject with ConnectTimeoutError
// within the deadline so the SDK can fall back to REST.
//
// Integration tests against an in-process mock gateway: a real `ws` server that
// either withholds `welcome` (blackhole) or sends it (healthy control).
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";

import { afterEach, describe, expect, it } from "vitest";
import { WebSocketServer, type WebSocket } from "ws";

import { ConnectTimeoutError, FalconTransport } from "../src/transport.js";

interface MockOpts {
  /** When true, the gateway accepts the socket but never sends `welcome`. */
  blackhole: boolean;
  /**
   * When set, the gateway sends `welcome` after this many ms — used to simulate a
   * `welcome` that arrives *after* the client's handshake deadline has fired.
   */
  welcomeDelayMs?: number;
}

/** A mock Falcon gateway that either stalls the handshake or completes it. */
function startMockGateway(opts: MockOpts): Promise<{
  restAddress: string;
  connections: () => number;
  close: () => Promise<void>;
}> {
  return new Promise((resolve) => {
    let connections = 0;
    const http: Server = createServer();
    const wss = new WebSocketServer({ server: http });
    const sockets: WebSocket[] = [];
    wss.on("connection", (ws) => {
      connections += 1;
      sockets.push(ws);
      const welcome = () =>
        ws.send(JSON.stringify({ type: "welcome", submissionCredits: 8, heartbeatMs: 0 }));
      if (!opts.blackhole) {
        welcome();
      } else if (opts.welcomeDelayMs !== undefined) {
        // Deliver `welcome` late, after the client's deadline should have fired.
        setTimeout(() => {
          if (ws.readyState === ws.OPEN) welcome();
        }, opts.welcomeDelayMs);
      }
      // blackhole (no delay): intentionally send nothing — the handshake is
      // complete but `welcome` never arrives.
    });
    http.listen(0, "127.0.0.1", () => {
      const { port } = http.address() as AddressInfo;
      resolve({
        restAddress: `http://127.0.0.1:${port}`,
        connections: () => connections,
        close: () =>
          new Promise<void>((done) => {
            for (const s of sockets) s.terminate();
            wss.close(() => http.close(() => done()));
          }),
      });
    });
  });
}

describe("FalconTransport handshake deadline", () => {
  let gw: Awaited<ReturnType<typeof startMockGateway>>;
  let t: FalconTransport | undefined;

  afterEach(async () => {
    t?.close();
    t = undefined;
    if (gw) await gw.close();
  });

  it("rejects with ConnectTimeoutError when welcome never arrives", async () => {
    gw = await startMockGateway({ blackhole: true });
    t = new FalconTransport(gw.restAddress, "/falcon", undefined, 80);

    const started = Date.now();
    await expect(t.connect()).rejects.toBeInstanceOf(ConnectTimeoutError);
    // Rejects around the deadline, not after some unrelated long stall.
    expect(Date.now() - started).toBeLessThan(2000);
    // The socket did open (the gateway saw the connection) — this is the
    // blackhole-after-upgrade case, not a refused dial.
    expect(gw.connections()).toBeGreaterThanOrEqual(1);
  });

  it("does not latch into a hung shared promise: each attempt is independently bounded", async () => {
    // A stalled handshake must be treated like a failed initial dial — it must
    // NOT spin up a background reconnect loop that a later connect() awaits
    // forever. Two sequential connects should each reject promptly.
    gw = await startMockGateway({ blackhole: true });
    t = new FalconTransport(gw.restAddress, "/falcon", undefined, 80);

    await expect(t.connect()).rejects.toBeInstanceOf(ConnectTimeoutError);
    const started = Date.now();
    await expect(t.connect()).rejects.toBeInstanceOf(ConnectTimeoutError);
    expect(Date.now() - started).toBeLessThan(2000);
    // A fresh dial was made for the second attempt (no stuck shared promise).
    expect(gw.connections()).toBeGreaterThanOrEqual(2);
  });

  it("ignores a late welcome that arrives after the deadline fired", async () => {
    // Race guard: once the handshake deadline rejects connect() with
    // ConnectTimeoutError and closes the socket, a `welcome` frame that slips in
    // before the close completes must NOT flip the transport to open (setting
    // `this.open`, arming the heartbeat, re-subscribing) behind the caller's back.
    gw = await startMockGateway({ blackhole: true, welcomeDelayMs: 120 });
    t = new FalconTransport(gw.restAddress, "/falcon", undefined, 60);

    await expect(t.connect()).rejects.toBeInstanceOf(ConnectTimeoutError);
    // Wait past the gateway's delayed welcome so it would have landed if the
    // socket were still selected.
    await new Promise((r) => setTimeout(r, 150));
    expect((t as unknown as { open: boolean }).open).toBe(false);
  });

  it("does not trip when welcome arrives before the deadline", async () => {
    // Control: a generous deadline must not reject a healthy handshake, and the
    // timer must be cleared so it never fires late against an open connection.
    gw = await startMockGateway({ blackhole: false });
    t = new FalconTransport(gw.restAddress, "/falcon", undefined, 1000);
    await expect(t.connect()).resolves.toBeUndefined();
  });

  it("waits indefinitely when the deadline is disabled (legacy behaviour)", async () => {
    // With no connect timeout, connect() must not self-reject; it settles only
    // when welcome finally arrives. Prove it by racing a never-resolving connect
    // against a short timer, then completing the handshake.
    gw = await startMockGateway({ blackhole: true });
    t = new FalconTransport(gw.restAddress, "/falcon"); // no connectTimeoutMs
    const race = await Promise.race([
      t.connect().then(() => "connected").catch((e) => `rejected:${(e as Error).name}`),
      new Promise<string>((r) => setTimeout(() => r("pending"), 200)),
    ]);
    expect(race).toBe("pending");
  });
});
