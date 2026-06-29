import { describe, expect, it } from "vitest";
import { commandStreamUrl, normalizeBase } from "../src/detect.js";
import { createCamundaClient } from "../src/index.js";

describe("detect", () => {
  it("normalizes base and builds ws url", () => {
    expect(normalizeBase("http://x:8080///")).toBe("http://x:8080");
    expect(commandStreamUrl("http://x:8080", "/command-stream")).toBe("ws://x:8080/command-stream");
    expect(commandStreamUrl("https://x", "/command-stream")).toBe("wss://x/command-stream");
  });
});

describe("createCamundaClient", () => {
  it("is a drop-in factory returning a client", () => {
    const c = createCamundaClient({ config: { CAMUNDA_AUTH_STRATEGY: "NONE", CAMUNDA_REST_ADDRESS: "http://localhost:8080", CAMUNDA_TRANSPORT: "rest" } });
    expect(typeof c.createProcessInstance).toBe("function");
    expect(typeof c.createJobWorker).toBe("function");
  });
});
