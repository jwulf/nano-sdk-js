// Falcon crash-resilience (issue #3): the transport must survive an engine
// crash/restart mid-session — reconnect with backoff, resubscribe, and resume
// jobs — WITHOUT ever emitting an unhandled promise rejection.
//
// These are integration tests against an in-process mock Falcon gateway (a real
// `ws` WebSocketServer on a throwaway localhost port). No live broker is needed:
// "crash" = tear the server down; "restart" = bind a fresh one to the same port,
// so the client's fixed URL resolves to the new process.
import { createServer, type Server } from "node:http";
import { createServer as netServer } from "node:net";
import type { AddressInfo } from "node:net";

import { afterEach, describe, expect, it } from "vitest";
import { WebSocketServer, type WebSocket } from "ws";

import { FalconTransport, type JobFrame } from "../src/transport.js";

/** A running mock Falcon gateway bound to a fixed port. */
interface MockGateway {
  restAddress: string;
  port: number;
  /** Number of client connections seen since start (proves a reconnect landed). */
  connections: () => number;
  /** Kill the gateway hard (terminate sockets + close listener) — the "crash". */
  crash: () => Promise<void>;
}

interface MockOpts {
  port: number;
  /** Submission-credit window advertised in `welcome`. */
  credits?: number;
  /** Push a `job` of this type to a worker as soon as it subscribes. */
  pushJobOnSubscribe?: string;
  /** Ack every createInstance with a 200 commandResult. */
  ackCreates?: boolean;
}

/** Reserve a free TCP port, then release it so the gateway can bind it. */
function reservePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = netServer();
    s.once("error", reject);
    s.listen(0, "127.0.0.1", () => {
      const { port } = s.address() as AddressInfo;
      s.close(() => resolve(port));
    });
  });
}

function startMockGateway(opts: MockOpts): Promise<MockGateway> {
  return new Promise((resolve, reject) => {
    let connections = 0;
    const http: Server = createServer();
    const wss = new WebSocketServer({ server: http });
    const sockets: WebSocket[] = [];
    wss.on("connection", (ws) => {
      connections += 1;
      sockets.push(ws);
      ws.send(JSON.stringify({ type: "welcome", submissionCredits: opts.credits ?? 8, heartbeatMs: 0 }));
      ws.on("message", (data) => {
        let frame: { type?: string; corr?: number; jobType?: string };
        try {
          frame = JSON.parse(data.toString());
        } catch {
          return;
        }
        if (frame.type === "subscribe" && opts.pushJobOnSubscribe && frame.jobType === opts.pushJobOnSubscribe) {
          ws.send(
            JSON.stringify({
              type: "job",
              job: { type: opts.pushJobOnSubscribe, jobKey: `${connections}`, processInstanceKey: "1", variables: {} },
            }),
          );
        }
        if (frame.type === "createInstance" && opts.ackCreates) {
          ws.send(JSON.stringify({ type: "commandResult", corr: frame.corr, status: 200, body: { processInstanceKey: "1" } }));
        }
      });
    });
    http.once("error", reject);
    http.listen(opts.port, "127.0.0.1", () => {
      resolve({
        restAddress: `http://127.0.0.1:${opts.port}`,
        port: opts.port,
        connections: () => connections,
        crash: () =>
          new Promise<void>((done) => {
            for (const s of sockets) s.terminate();
            wss.close(() => http.close(() => done()));
          }),
      });
    });
  });
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Poll `predicate` until true or `timeoutMs` elapses. */
async function waitFor(predicate: () => boolean, timeoutMs = 4000, stepMs = 20): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return true;
    await sleep(stepMs);
  }
  return predicate();
}

describe("FalconTransport crash-resilient reconnect (issue #3)", () => {
  let gw: MockGateway | undefined;
  let t: FalconTransport | undefined;
  let unhandled: unknown[] = [];
  let onUnhandled: ((e: unknown) => void) | undefined;

  const trackUnhandled = () => {
    unhandled = [];
    onUnhandled = (e: unknown) => unhandled.push(e);
    process.on("unhandledRejection", onUnhandled);
  };

  afterEach(async () => {
    if (onUnhandled) process.off("unhandledRejection", onUnhandled);
    onUnhandled = undefined;
    t?.close();
    t = undefined;
    if (gw) await gw.crash();
    gw = undefined;
    // Let any in-flight microtasks (e.g. a stray rejection) surface before asserting.
    await sleep(10);
  });

  it("resubscribes and resumes jobs after the engine crashes and restarts", async () => {
    const port = await reservePort();
    trackUnhandled();

    gw = await startMockGateway({ port, pushJobOnSubscribe: "greet" });
    const jobs: JobFrame[] = [];
    t = new FalconTransport(gw.restAddress, "/falcon");
    await t.subscribe({
      jobType: "greet",
      worker: "w1",
      credits: 4,
      timeoutMs: 1000,
      fetchVariables: null,
      onJob: (job) => jobs.push(job),
    });

    // First delivery proves the live path before we crash the server.
    expect(await waitFor(() => jobs.length >= 1)).toBe(true);
    const before = jobs.length;

    // Crash the engine mid-session, then bring it back on the same port after a
    // short outage (longer than the first backoff, so the client retries).
    await gw.crash();
    await sleep(250);
    gw = await startMockGateway({ port, pushJobOnSubscribe: "greet" });

    // The client must reconnect on its own, resubscribe, and receive a new job.
    expect(await waitFor(() => jobs.length > before, 5000)).toBe(true);
    expect(gw.connections()).toBeGreaterThanOrEqual(1);
    // The crash+restart never produced an unhandled rejection.
    expect(unhandled).toEqual([]);
  });

  it("keeps retrying without an unhandled rejection while the engine stays down", async () => {
    const port = await reservePort();
    trackUnhandled();

    gw = await startMockGateway({ port });
    t = new FalconTransport(gw.restAddress, "/falcon");
    // Open the socket (a create acquires the welcome credit window).
    await t.connect();

    // Crash and DO NOT restart: the background loop should keep retrying failed
    // connects, swallowing each failure rather than leaking an unhandled rejection.
    await gw.crash();
    gw = undefined;
    // Span several backoff cycles (base 100ms, cap 2000ms) against a dead port.
    await sleep(1200);
    expect(unhandled).toEqual([]);

    // A clean close() must stop the loop (no further reconnects) and stay quiet.
    t.close();
    await sleep(300);
    expect(unhandled).toEqual([]);
  });

  it("surfaces a fast initial-connect failure (reject-once) when the engine is absent", async () => {
    const port = await reservePort();
    trackUnhandled();

    // Nothing is listening on `port`: the very first connect should reject
    // promptly instead of silently looping.
    t = new FalconTransport(`http://127.0.0.1:${port}`, "/falcon");
    await expect(t.connect()).rejects.toThrow(/falcon (connect failed|closed before ready)/);
    t.close();
    await sleep(50);
    expect(unhandled).toEqual([]);
  });

  it("does NOT start a background reconnect loop after a failed initial connect", async () => {
    const port = await reservePort();
    trackUnhandled();

    // A failed initial dial must reject once and stop — not spin a hidden loop.
    t = new FalconTransport(`http://127.0.0.1:${port}`, "/falcon");
    await expect(t.connect()).rejects.toThrow();

    // Now bring an engine up on that port. Because no background loop is running,
    // the client must NOT connect to it on its own (the caller never re-dialed).
    gw = await startMockGateway({ port });
    await sleep(600); // span several would-be backoff cycles
    expect(gw.connections()).toBe(0);
    expect(unhandled).toEqual([]);
  });
});
