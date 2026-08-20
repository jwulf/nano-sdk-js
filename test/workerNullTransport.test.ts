// Guards the NanoJobWorker lifecycle invariant that regressed in issue #12:
// createJobWorker's Falcon/auto path constructs the worker with a null transport
// and binds it asynchronously, so start() must be null-safe (defer until bound)
// and idempotent (a proxy self-start plus an eager caller start subscribe once).
import { describe, expect, it, vi } from "vitest";
import { NanoJobWorker } from "../src/nanoWorker.js";
import type { FalconTransport, Subscription } from "../src/transport.js";

function fakeTransport() {
  return {
    subscribe: vi.fn(async (_sub: Subscription) => {}),
    unsubscribe: vi.fn(),
    grantCredits: vi.fn(),
    completeJob: vi.fn(),
    failJob: vi.fn(),
    throwError: vi.fn(),
  } as unknown as FalconTransport & {
    subscribe: ReturnType<typeof vi.fn>;
    unsubscribe: ReturnType<typeof vi.fn>;
  };
}

const cfg = {
  jobType: "demo",
  autoStart: false as const,
  jobHandler: async () => undefined as never,
};

describe("NanoJobWorker null-transport lifecycle (issue #12)", () => {
  it("defers start() called before bindTransport(), subscribing exactly once on bind", async () => {
    const t = fakeTransport();
    const w = new NanoJobWorker(null, cfg);

    // start() in the null-transport window must not throw.
    await expect(w.start()).resolves.toBeUndefined();
    expect(t.subscribe).not.toHaveBeenCalled();

    w.bindTransport(t);
    await Promise.resolve();

    expect(t.subscribe).toHaveBeenCalledTimes(1);
  });

  it("is idempotent: proxy self-start + eager caller start subscribe once", async () => {
    const t = fakeTransport();
    const w = new NanoJobWorker(null, cfg);

    // Simulate the eager, duplicate start() from the caller-side adapter.
    await w.start();
    await w.start();

    w.bindTransport(t);
    await Promise.resolve();

    // A duplicate start() after bind must not add a second subscription.
    await w.start();

    expect(t.subscribe).toHaveBeenCalledTimes(1);
  });

  it("subscribes once when start() is called twice after bind", async () => {
    const t = fakeTransport();
    const w = new NanoJobWorker(t, cfg);

    await w.start();
    await w.start();

    expect(t.subscribe).toHaveBeenCalledTimes(1);
  });

  it("does not re-subscribe on bind when start() was never requested", async () => {
    const t = fakeTransport();
    const w = new NanoJobWorker(null, cfg);

    w.bindTransport(t);
    await Promise.resolve();

    expect(t.subscribe).not.toHaveBeenCalled();
  });

  it("allows a fresh start()/subscribe after stop()", async () => {
    const t = fakeTransport();
    const w = new NanoJobWorker(t, cfg);

    await w.start();
    w.stop();
    expect(t.unsubscribe).toHaveBeenCalledTimes(1);

    await w.start();
    expect(t.subscribe).toHaveBeenCalledTimes(2);
  });

  it("surfaces a subscribe() failure to the caller and resets so a retry can resubscribe", async () => {
    const t = fakeTransport();
    t.subscribe
      .mockRejectedValueOnce(new Error("boom"))
      .mockResolvedValueOnce(undefined);
    const w = new NanoJobWorker(t, cfg);

    // The failure must propagate, not be swallowed into a wedged "subscribed" state.
    await expect(w.start()).rejects.toThrow("boom");

    // A subsequent start() must be able to retry (shared promise was reset).
    await expect(w.start()).resolves.toBeUndefined();
    expect(t.subscribe).toHaveBeenCalledTimes(2);
  });

  it("coalesces racing start() calls onto one shared in-flight subscribe attempt", async () => {
    const t = fakeTransport();
    let resolveSub: () => void = () => {};
    t.subscribe.mockImplementationOnce(
      () => new Promise<void>((res) => (resolveSub = res)),
    );
    const w = new NanoJobWorker(t, cfg);

    const a = w.start();
    const b = w.start();
    resolveSub();
    await Promise.all([a, b]);

    expect(t.subscribe).toHaveBeenCalledTimes(1);
  });
});
