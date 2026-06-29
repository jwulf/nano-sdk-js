// Picks a WebSocket implementation: native global (browser/Deno/Node>=22) or the
// `ws` package (Node). Kept tiny so the transport stays runtime-agnostic.
export type WSImpl = new (url: string) => WebSocket;

let cached: WSImpl | null = null;

export async function getWebSocket(): Promise<WSImpl> {
  if (cached) return cached;
  const g = globalThis as { WebSocket?: WSImpl };
  if (typeof g.WebSocket === "function") {
    cached = g.WebSocket;
    return cached;
  }
  const mod = await import("ws");
  cached = (mod.default ?? mod.WebSocket) as unknown as WSImpl;
  return cached;
}
