import { describe, expect, it } from "vitest";
import { falconUrl, normalizeBase } from "../src/detect.js";
import { createCamundaClient } from "../src/index.js";

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
