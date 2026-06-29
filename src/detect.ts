// Nano server detection. A nanobpmn gateway advertises itself on GET /v2/topology
// via a `nano` block carrying `commandStreamPath`. Detecting it lets the SDK
// upgrade to the command-stream protocol transparently.

export interface NanoInfo {
  engine: string;
  version?: string;
  commandStreamPath: string;
}

const cache = new Map<string, NanoInfo | null>();

/** Strip trailing slashes for a tidy REST base. */
export function normalizeBase(url: string): string {
  return url.replace(/\/+$/, "");
}

/**
 * Probes a REST address once (cached) and returns the Nano engine info if the
 * server is a nanobpmn gateway, or null for stock Camunda 8.
 */
export async function detectNano(
  restAddress: string,
  fetchImpl: typeof fetch = fetch,
): Promise<NanoInfo | null> {
  const base = normalizeBase(restAddress);
  if (cache.has(base)) return cache.get(base) ?? null;
  let info: NanoInfo | null = null;
  try {
    const res = await fetchImpl(`${base}/v2/topology`, {
      headers: { accept: "application/json" },
    });
    if (res.ok) {
      const body: any = await res.json().catch(() => ({}));
      const nano = body?.nano;
      if (nano && typeof nano.commandStreamPath === "string") {
        info = {
          engine: String(nano.engine ?? "nanobpmn"),
          version: nano.version ? String(nano.version) : undefined,
          commandStreamPath: nano.commandStreamPath,
        };
      }
    }
  } catch {
    info = null; // unreachable or non-Nano: stay on REST
  }
  cache.set(base, info);
  return info;
}

/** Test seam: clear the detection cache. */
export function _clearDetectionCache(): void {
  cache.clear();
}

/** Build the ws:// or wss:// command-stream URL from a REST base + path. */
export function commandStreamUrl(restAddress: string, path: string): string {
  let base = normalizeBase(restAddress);
  if (base.startsWith("https://")) base = "wss://" + base.slice("https://".length);
  else if (base.startsWith("http://")) base = "ws://" + base.slice("http://".length);
  return base + path;
}
