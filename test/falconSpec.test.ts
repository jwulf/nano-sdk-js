import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";

import { afterEach, describe, expect, it } from "vitest";
import { WebSocketServer, type WebSocket } from "ws";

import { FalconTransport } from "../src/transport.js";
import type { ServerFrameType } from "../src/generated/falconFrames.js";

// Every documented server frame the client must tolerate. Kept in sync with the
// generated `ServerFrameType` by the compile-time assignment below — if the spec
// grows a server frame, this array must grow too or TypeScript errors.
const SERVER_FRAME_TYPES = [
  "welcome",
  "job",
  "commandResult",
  "instanceCompleted",
  "submissionCredits",
  "pressure",
  "workerAdvice",
  "heartbeat",
] as const;
// Compile-time coverage: the array is exactly the generated union.
const _coverage: readonly ServerFrameType[] = SERVER_FRAME_TYPES;
type _Exhaustive = Exclude<ServerFrameType, (typeof SERVER_FRAME_TYPES)[number]>;
const _never: _Exhaustive extends never ? true : never = true;
void _coverage;
void _never;

/**
 * Mock Falcon gateway that, after `welcome`, sprays advisory + unknown frames
 * *before* acking a create — proving the client tolerates (ignores) them and
 * the create still resolves.
 */
function startNoisyGateway(): Promise<{ restAddress: string; close: () => Promise<void> }> {
  return new Promise((resolve) => {
    const http: Server = createServer();
    const wss = new WebSocketServer({ server: http });
    const sockets: WebSocket[] = [];
    wss.on("connection", (ws) => {
      sockets.push(ws);
      ws.send(JSON.stringify({ type: "welcome", submissionCredits: 8, heartbeatMs: 0 }));
      ws.on("message", (data) => {
        const f = JSON.parse(String(data));
        if (f.type === "createInstance") {
          // Advisory + genuinely unknown noise, then the real ack.
          ws.send(JSON.stringify({ type: "workerAdvice", recommendedConcurrency: 4 }));
          ws.send(JSON.stringify({ type: "pressure", level: "high", retryAfterMs: 10 }));
          ws.send(JSON.stringify({ type: "heartbeat" }));
          ws.send(JSON.stringify({ type: "futureFrameFromANewerServer", data: 123 }));
          ws.send(JSON.stringify({ type: "commandResult", corr: f.corr, status: 200, body: { processInstanceKey: "42" } }));
        }
      });
    });
    http.listen(0, () => {
      const port = (http.address() as AddressInfo).port;
      resolve({
        restAddress: `http://127.0.0.1:${port}`,
        close: () =>
          new Promise<void>((r) => {
            for (const s of sockets) s.close();
            wss.close(() => http.close(() => r()));
          }),
      });
    });
  });
}

describe("falcon spec: server-frame tolerance", () => {
  let gw: { restAddress: string; close: () => Promise<void> } | null = null;
  afterEach(async () => {
    await gw?.close();
    gw = null;
  });

  it("ignores advisory and unknown server frames, still resolving the create", async () => {
    gw = await startNoisyGateway();
    const t = new FalconTransport(gw.restAddress, "/falcon");
    const r = await t.createInstance({ processDefinitionId: "demo" });
    expect(r.status).toBe(200);
    expect((r.body as { processInstanceKey: string }).processInstanceKey).toBe("42");
    t.close();
  });

  it("documents workerAdvice among the server frame types (drift regression)", () => {
    // Regression for the spec drift where `workerAdvice` was broadcast to
    // clients but absent from the spec/types.
    expect(SERVER_FRAME_TYPES).toContain("workerAdvice");
  });
});
