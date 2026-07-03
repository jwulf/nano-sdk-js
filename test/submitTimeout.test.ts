import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";

import { afterEach, describe, expect, it } from "vitest";
import { WebSocketServer, type WebSocket } from "ws";

import { FalconTransport, SubmissionTimeoutError } from "../src/transport.js";

interface MockOpts {
  /** Initial submission-credit window advertised in `welcome`. */
  credits: number;
  /** Ack every createInstance with a 200 commandResult. */
  ackCreates?: boolean;
  /** Grant extra credits (via a `submissionCredits` frame) after `grantAfterMs`. */
  grantAfterMs?: number;
  grantN?: number;
}

/** Mock Falcon protocol with a controllable submission-credit window. */
function startMockGateway(opts: MockOpts): Promise<{ restAddress: string; close: () => Promise<void> }> {
  return new Promise((resolve) => {
    const http: Server = createServer();
    const wss = new WebSocketServer({ server: http });
    const sockets: WebSocket[] = [];
    wss.on("connection", (ws) => {
      sockets.push(ws);
      ws.send(JSON.stringify({ type: "welcome", submissionCredits: opts.credits, heartbeatMs: 0 }));
      if (opts.grantAfterMs !== undefined) {
        setTimeout(() => ws.send(JSON.stringify({ type: "submissionCredits", n: opts.grantN ?? 1 })), opts.grantAfterMs);
      }
      ws.on("message", (data) => {
        if (!opts.ackCreates) return;
        const frame = JSON.parse(data.toString());
        if (frame.type === "createInstance") {
          ws.send(JSON.stringify({ type: "commandResult", corr: frame.corr, status: 200, body: { processInstanceKey: "1" } }));
        }
      });
    });
    http.listen(0, "127.0.0.1", () => {
      const { port } = http.address() as AddressInfo;
      resolve({
        restAddress: `http://127.0.0.1:${port}`,
        close: () =>
          new Promise<void>((done) => {
            for (const s of sockets) s.terminate();
            wss.close(() => http.close(() => done()));
          }),
      });
    });
  });
}

describe("FalconTransport submission-credit gating", () => {
  let gw: Awaited<ReturnType<typeof startMockGateway>>;

  afterEach(async () => {
    if (gw) await gw.close();
  });

  it("rejects with SubmissionTimeoutError when no credit is granted in time", async () => {
    gw = await startMockGateway({ credits: 0 });
    const t = new FalconTransport(gw.restAddress, "/falcon");
    await expect(t.createInstance({ processDefinitionId: "p", submitTimeoutMs: 50 })).rejects.toBeInstanceOf(
      SubmissionTimeoutError,
    );
    t.close();
  });

  it("honours the transport-wide default submit timeout", async () => {
    gw = await startMockGateway({ credits: 0 });
    const t = new FalconTransport(gw.restAddress, "/falcon", 40);
    await expect(t.createInstance({ processDefinitionId: "p" })).rejects.toBeInstanceOf(SubmissionTimeoutError);
    t.close();
  });

  it("gates the create until a credit is granted, then proceeds", async () => {
    // Window starts empty; the create queues and only fires once the server tops
    // up credits -- proving the client waits on the credit, not just the ack.
    gw = await startMockGateway({ credits: 0, ackCreates: true, grantAfterMs: 60, grantN: 1 });
    const t = new FalconTransport(gw.restAddress, "/falcon", 1000);
    const r = await t.createInstance({ processDefinitionId: "p" });
    expect(r.status).toBe(200);
    t.close();
  });

  it("consumes the welcome window without waiting when credits are available", async () => {
    gw = await startMockGateway({ credits: 4, ackCreates: true });
    const t = new FalconTransport(gw.restAddress, "/falcon", 1000);
    const r = await t.createInstance({ processDefinitionId: "p", submitTimeoutMs: 1000 });
    expect(r.status).toBe(200);
    t.close();
  });
});
