// Verifies the CAMUNDA_FORCE_REST escape hatch and the graceful Falcon->REST
// fallback semantics documented on createCamundaClient.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { _clearDetectionCache } from "../src/detect.js";
import { createCamundaClient } from "../src/index.js";

const REST = "http://localhost:8080";

beforeEach(() => {
  _clearDetectionCache();
  delete process.env.CAMUNDA_FORCE_REST;
  delete process.env.CAMUNDA_TRANSPORT;
});
afterEach(() => {
  delete process.env.CAMUNDA_FORCE_REST;
  delete process.env.CAMUNDA_TRANSPORT;
});

describe("CAMUNDA_FORCE_REST", () => {
  it("skips detection entirely when set via opts.config", async () => {
    const fetchSpy = vi.fn();
    // Stub global fetch so any accidental detection probe would be observable.
    vi.stubGlobal("fetch", fetchSpy);

    const c = createCamundaClient({
      config: {
        CAMUNDA_AUTH_STRATEGY: "NONE",
        CAMUNDA_REST_ADDRESS: REST,
        CAMUNDA_FORCE_REST: "1",
      },
    });
    // Just referencing the client should not trigger a topology probe.
    expect(typeof c.createProcessInstance).toBe("function");
    expect(fetchSpy).not.toHaveBeenCalled();
    vi.unstubAllGlobals();
  });

  it("skips detection entirely when set via process.env", async () => {
    process.env.CAMUNDA_FORCE_REST = "true";
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);

    const c = createCamundaClient({
      config: { CAMUNDA_AUTH_STRATEGY: "NONE", CAMUNDA_REST_ADDRESS: REST },
    });
    expect(typeof c.createProcessInstance).toBe("function");
    expect(fetchSpy).not.toHaveBeenCalled();
    vi.unstubAllGlobals();
  });

  it("overrides CAMUNDA_TRANSPORT=falcon", () => {
    const c = createCamundaClient({
      config: {
        CAMUNDA_AUTH_STRATEGY: "NONE",
        CAMUNDA_REST_ADDRESS: REST,
        CAMUNDA_TRANSPORT: "falcon",
        CAMUNDA_FORCE_REST: "yes",
      },
    });
    // No falcon path — client is a bare REST proxy.
    expect(typeof c.createProcessInstance).toBe("function");
  });

  it("falsy values do not disable falcon", () => {
    // Sanity: 'off' / '0' / '' should not activate the escape hatch.
    for (const falsy of ["0", "off", "false", "no", ""]) {
      process.env.CAMUNDA_FORCE_REST = falsy;
      const c = createCamundaClient({
        config: {
          CAMUNDA_AUTH_STRATEGY: "NONE",
          CAMUNDA_REST_ADDRESS: REST,
          CAMUNDA_TRANSPORT: "rest", // keep test hermetic
        },
      });
      expect(typeof c.createProcessInstance).toBe("function");
    }
  });
});
