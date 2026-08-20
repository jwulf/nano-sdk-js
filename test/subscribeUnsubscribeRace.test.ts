// Deterministic test for the subscribe/unsubscribe race (issue #12 review):
// FalconTransport.subscribe() awaits connect() before sending the subscribe
// frame. If stop()/unsubscribe() runs during that window, the sub is no longer
// wanted — sending the subscribe anyway would make the gateway activate/push
// jobs the client silently drops (unsubscribe() only clears the local handler),
// causing avoidable job timeouts. We drive the transport with a controllable
// fake WebSocket so we can interleave unsubscribe() while connect() is pending.
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
  emit(obj: unknown): void {
    this.onmessage?.({ data: JSON.stringify(obj) });
  }
  hasFrame(type: string): boolean {
    return this.sent.some((s) => JSON.parse(s).type === type);
  }
  countFrame(type: string): number {
    return this.sent.filter((s) => JSON.parse(s).type === type).length;
  }
}

(globalThis as { WebSocket?: unknown }).WebSocket = FakeWS as unknown;

const { FalconTransport } = await import("../src/transport.js");
import type { Subscription } from "../src/transport.js";

const tick = () => new Promise<void>((r) => setTimeout(r, 0));

const sub: Subscription = {
  jobType: "demo",
  worker: "w",
  credits: 5,
  timeoutMs: 1000,
  fetchVariables: null,
  onJob: () => {},
};

describe("FalconTransport subscribe/unsubscribe race (issue #12)", () => {
  beforeEach(() => {
    FakeWS.instances = [];
  });

  it("does not send a subscribe frame if unsubscribe() ran while connect() was pending", async () => {
    const t = new FalconTransport("http://fake/", "/falcon");

    // subscribe() registers the sub and awaits connect() (welcome not yet in).
    const p = t.subscribe(sub);
    await tick();
    const ws = FakeWS.instances[0];

    // stop()/unsubscribe() lands during the connect window.
    t.unsubscribe("demo");

    // connect() now completes.
    ws.emit({ type: "welcome", submissionCredits: 3, heartbeatMs: 0 });
    await p;

    // The stale subscribe must NOT reach the gateway.
    expect(ws.hasFrame("subscribe")).toBe(false);

    t.close();
  });

  it("still sends the subscribe frame when the sub remains active through connect()", async () => {
    const t = new FalconTransport("http://fake/", "/falcon");

    const p = t.subscribe(sub);
    await tick();
    const ws = FakeWS.instances[0];

    ws.emit({ type: "welcome", submissionCredits: 3, heartbeatMs: 0 });
    await p;

    expect(ws.hasFrame("subscribe")).toBe(true);

    t.close();
  });

  it("sends exactly one subscribe frame when subscribing before connect (no duplicate from welcome)", async () => {
    // The `welcome` handler re-subscribes every active sub. subscribe() must not
    // also send its own frame in that window, or the gateway sees a duplicate.
    const t = new FalconTransport("http://fake/", "/falcon");

    const p = t.subscribe(sub);
    await tick();
    const ws = FakeWS.instances[0];

    ws.emit({ type: "welcome", submissionCredits: 3, heartbeatMs: 0 });
    await p;

    expect(ws.countFrame("subscribe")).toBe(1);

    t.close();
  });

  it("sends the subscribe frame when subscribing after the transport is already open", async () => {
    const t = new FalconTransport("http://fake/", "/falcon");

    // Bring the transport up with no active subs.
    const first = t.connect();
    await tick();
    const ws = FakeWS.instances[0];
    ws.emit({ type: "welcome", submissionCredits: 3, heartbeatMs: 0 });
    await first;
    expect(ws.countFrame("subscribe")).toBe(0);

    // Now subscribe against the already-open transport: welcome won't fire again,
    // so subscribe() itself must send exactly one frame.
    await t.subscribe(sub);
    expect(ws.countFrame("subscribe")).toBe(1);

    t.close();
  });
});
