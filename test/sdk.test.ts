import { describe, expect, it } from "vitest";
import { falconUrl, normalizeBase } from "../src/detect.js";
import { createCamundaClient } from "../src/index.js";
import { withRestPollDefault, wrapRestPollDefault } from "../src/restPollDefault.js";

describe("detect", () => {
  it("normalizes base and builds ws url", () => {
    expect(normalizeBase("http://x:8080///")).toBe("http://x:8080");
    expect(falconUrl("http://x:8080", "/falcon")).toBe("ws://x:8080/falcon");
    expect(falconUrl("https://x", "/falcon")).toBe("wss://x/falcon");
  });
});

describe("createCamundaClient", () => {
  it("is a drop-in factory returning a client", () => {
    const c = createCamundaClient({ config: { CAMUNDA_AUTH_STRATEGY: "NONE", CAMUNDA_REST_ADDRESS: "http://localhost:8080", CAMUNDA_TRANSPORT: "rest" } });
    expect(typeof c.createProcessInstance).toBe("function");
    expect(typeof c.createJobWorker).toBe("function");
  });
});

describe("REST long-poll default", () => {
  it("injects a 30s pollTimeoutMs when the caller omits one", () => {
    const cfg = withRestPollDefault({ jobType: "x" });
    expect(cfg.pollTimeoutMs).toBe(30_000);
  });

  it("respects an explicit pollTimeoutMs (including 0 and negative)", () => {
    expect(withRestPollDefault({ jobType: "x", pollTimeoutMs: 5_000 }).pollTimeoutMs).toBe(5_000);
    expect(withRestPollDefault({ jobType: "x", pollTimeoutMs: 0 }).pollTimeoutMs).toBe(0);
    expect(withRestPollDefault({ jobType: "x", pollTimeoutMs: -1 }).pollTimeoutMs).toBe(-1);
  });

  it("passes through non-worker methods unchanged and defaults createJobWorker", () => {
    const seen: any[] = [];
    const base = {
      createJobWorker: (cfg: any) => { seen.push(cfg); return { started: true }; },
      createProcessInstance: (input: any) => ({ echoed: input }),
    };
    const wrapped = wrapRestPollDefault(base as any);
    wrapped.createJobWorker({ jobType: "x" });
    expect(seen[0].pollTimeoutMs).toBe(30_000);
    // untouched pass-through
    expect((wrapped as any).createProcessInstance({ id: 1 })).toEqual({ echoed: { id: 1 } });
    // explicit value preserved through the proxy
    wrapped.createJobWorker({ jobType: "y", pollTimeoutMs: 1234 });
    expect(seen[1].pollTimeoutMs).toBe(1234);
  });
});
