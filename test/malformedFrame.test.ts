import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";

import { afterEach, describe, expect, it } from "vitest";
import { WebSocketServer, type WebSocket } from "ws";

import { FalconTransport, MalformedFrameError } from "../src/transport.js";

type AckMode =
  | "missingStatus" // commandResult with no `status` -> must not be read as success(0)
  | "missingKeyOnComplete"; // instanceCompleted with no `processInstanceKey` -> must not become ""

/**
 * A mock gateway that deliberately emits malformed result frames, to prove the
 * transport fails loud at the decode boundary instead of laundering the missing
 * field into a benign-looking default ("" / 0) that sails past downstream checks.
 */
function startMockGateway(mode: AckMode): Promise<{ restAddress: string; close: () => Promise<void> }> {
  return new Promise((resolve) => {
    const http: Server = createServer();
    const wss = new WebSocketServer({ server: http });
    const sockets: WebSocket[] = [];
    wss.on("connection", (ws) => {
      sockets.push(ws);
      ws.send(JSON.stringify({ type: "welcome", submissionCredits: 8, heartbeatMs: 0 }));
      ws.on("message", (data) => {
        const frame = JSON.parse(data.toString());
        if (frame.type !== "createInstance") return;
        if (mode === "missingStatus") {
          // No `status` field at all.
          ws.send(JSON.stringify({ type: "commandResult", corr: frame.corr, body: { processInstanceKey: "1" } }));
        } else if (mode === "missingKeyOnComplete") {
          ws.send(JSON.stringify({ type: "commandResult", corr: frame.corr, status: 200, body: { processInstanceKey: "1" } }));
          // Completion frame lacks `processInstanceKey`.
          ws.send(JSON.stringify({ type: "instanceCompleted", corr: frame.corr, processCompleted: true, variables: {} }));
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

describe("FalconTransport strict frame decode", () => {
  let gw: Awaited<ReturnType<typeof startMockGateway>>;

  afterEach(async () => {
    if (gw) await gw.close();
  });

  it("rejects a commandResult that omits status instead of defaulting to 0", async () => {
    gw = await startMockGateway("missingStatus");
    const t = new FalconTransport(gw.restAddress, "/falcon", 1000);
    await expect(t.createInstance({ processDefinitionId: "p" })).rejects.toBeInstanceOf(MalformedFrameError);
    t.close();
  });

  it("rejects an instanceCompleted that omits processInstanceKey instead of defaulting to ''", async () => {
    gw = await startMockGateway("missingKeyOnComplete");
    const t = new FalconTransport(gw.restAddress, "/falcon", 1000);
    await expect(
      t.createInstance({ processDefinitionId: "p", awaitCompletion: true }),
    ).rejects.toBeInstanceOf(MalformedFrameError);
    t.close();
  });
});
