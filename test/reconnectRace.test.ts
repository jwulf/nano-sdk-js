// Deterministic test for the stale-socket race (issue #3 review round 3): a
// superseded WebSocket's late `onclose` must not reset `open`/credits/pending on
// the *current* connection. We drive the transport with a controllable fake
// WebSocket (installed as the global impl) so we can interleave a stale socket's
// close after a newer socket is already open — an ordering that is impossible to
// force deterministically with a real `ws` server.
import { beforeEach, describe, expect, it } from "vitest";

class FakeWS {
  static instances: FakeWS[] = [];
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: (() => void) | null = null;
  sent: string[] = [];
  closed = false;
  constructor(public url: string) {
    FakeWS.instances.push(this);
  }
  send(s: string): void {
    this.sent.push(s);
  }
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.onclose?.();
  }
  emitError(): void {
    this.onerror?.();
  }
  emitClose(): void {
    this.onclose?.();
  }
  emit(obj: unknown): void {
    this.onmessage?.({ data: JSON.stringify(obj) });
  }
  lastFrame(type: string): Record<string, unknown> | undefined {
    for (let i = this.sent.length - 1; i >= 0; i--) {
      const f = JSON.parse(this.sent[i]);
      if (f.type === type) return f;
    }
    return undefined;
  }
}

// Install the fake as the global WebSocket BEFORE the transport's ws.ts caches an
// impl. This whole file uses the fake (isolated in its own vitest worker).
(globalThis as { WebSocket?: unknown }).WebSocket = FakeWS as unknown;

const { FalconTransport } = await import("../src/transport.js");

/** Flush the microtask/`getWebSocket()` chain so the socket instance exists. */
const tick = () => new Promise<void>((r) => setTimeout(r, 0));

describe("FalconTransport stale-socket race (issue #3, round 3)", () => {
  beforeEach(() => {
    FakeWS.instances = [];
  });

  it("ignores a superseded socket's late onclose (keeps the current connection open)", async () => {
    const t = new FalconTransport("http://fake/", "/falcon");

    // Attempt 1 (socket A): initial connect fails pre-`welcome` (engine down).
    const p1 = t.connect();
    await tick();
    const a = FakeWS.instances[0];
    a.emitError();
    await expect(p1).rejects.toThrow();

    // Attempt 2 (socket B): caller retries and this one reaches `welcome`.
    const p2 = t.connect();
    await tick();
    const b = FakeWS.instances[1];
    expect(b).not.toBe(a);
    b.emit({ type: "welcome", submissionCredits: 3, heartbeatMs: 0 });
    await p2;

    // The stale socket A now closes *late* (after B is the active socket). With
    // the guard this must NOT touch B's state; without it, open/credits/pending
    // for the live connection would be wiped.
    a.emitClose();
    await tick();

    // Proof B is still healthy: a create consumes a granted credit, is sent on B,
    // and resolves on B's ack. (If A's close had wiped state, connect() would see
    // !open and reject, or the create would stall with no credits.)
    const cr = t.createInstance({ processDefinitionId: "p" });
    await tick();
    const frame = b.lastFrame("createInstance");
    expect(frame).toBeDefined();
    b.emit({ type: "commandResult", corr: frame!.corr, status: 200, body: { processInstanceKey: "1" } });
    const res = await cr;
    expect(res.status).toBe(200);

    t.close();
  });
});
