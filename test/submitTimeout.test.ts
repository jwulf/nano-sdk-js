import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";

import { afterEach, describe, expect, it } from "vitest";
import { WebSocketServer, type WebSocket } from "ws";

import { CommandStreamTransport, SubmissionTimeoutError } from "../src/transport.js";

/**
 * Mock command stream that welcomes the client but (by default) never acks a
 * createInstance, so we can drive the submit-timeout path deterministically.
 */
function startMockGateway(ackCreates: boolean): Promise<{
  restAddress: string;
  close: () => Promise<void>;
}> {
  return new Promise((resolve) => {
    const http: Server = createServer();
    const wss = new WebSocketServer({ server: http });
    const sockets: WebSocket[] = [];
    wss.on("connection", (ws) => {
      sockets.push(ws);
      ws.send(JSON.stringify({ type: "welcome", submissionCredits: 256, heartbeatMs: 0 }));
      ws.on("message", (data) => {
        if (!ackCreates) return;
        const frame = JSON.parse(data.toString());
        if (frame.type === "createInstance") {
          ws.send(
            JSON.stringify({
              type: "commandResult",
              corr: frame.corr,
              status: 200,
              body: { processInstanceKey: "1" },
            }),
          );
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

describe("CommandStreamTransport submit timeout", () => {
  let gw: Awaited<ReturnType<typeof startMockGateway>>;

  afterEach(async () => {
    if (gw) await gw.close();
  });

  it("rejects with SubmissionTimeoutError when the gateway never acks in time", async () => {
    gw = await startMockGateway(false);
    const t = new CommandStreamTransport(gw.restAddress, "/command-stream");
    await expect(
      t.createInstance({ processDefinitionId: "p", submitTimeoutMs: 50 }),
    ).rejects.toBeInstanceOf(SubmissionTimeoutError);
    t.close();
  });

  it("honours the transport-wide default submit timeout", async () => {
    gw = await startMockGateway(false);
    const t = new CommandStreamTransport(gw.restAddress, "/command-stream", 40);
    await expect(t.createInstance({ processDefinitionId: "p" })).rejects.toBeInstanceOf(
      SubmissionTimeoutError,
    );
    t.close();
  });

  it("resolves normally (no timeout) when the gateway acks", async () => {
    gw = await startMockGateway(true);
    const t = new CommandStreamTransport(gw.restAddress, "/command-stream", 1000);
    const r = await t.createInstance({ processDefinitionId: "p", submitTimeoutMs: 1000 });
    expect(r.status).toBe(200);
    t.close();
  });
});
