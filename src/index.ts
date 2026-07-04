// @nanobpm/nano-sdk — a drop-in replacement for @camunda8/orchestration-cluster-api.
//
// It re-exports the entire upstream SDK surface, then overrides
// `createCamundaClient` (and the default export) so that, when connected to a
// Nano server, process-instance creation and job workers transparently upgrade
// to Nano's Falcon protocol. Against stock Camunda 8 it is a no-op: the
// upstream REST behaviour is used unchanged.
import {
  createCamundaClient as createCamundaClientBase,
} from "@camunda8/orchestration-cluster-api";
import { detectNano, normalizeBase, type NanoInfo } from "./detect.js";
import { FalconTransport } from "./transport.js";
import { EmbeddedTransport, type EmbeddedHost } from "./embedded.js";
import { NanoJobWorker } from "./nanoWorker.js";

// Re-export everything else so consumers can swap the import path and nothing
// breaks.
export * from "@camunda8/orchestration-cluster-api";
export { detectNano, type NanoInfo } from "./detect.js";
export { FalconTransport, SubmissionTimeoutError } from "./transport.js";
export { EmbeddedTransport, type EmbeddedHost, type EmbeddedJob } from "./embedded.js";
export { NanoJobWorker } from "./nanoWorker.js";

/** auto: upgrade only on Nano. falcon: force. rest: never upgrade. embedded: in-process μ-nano. */
export type NanoTransport = "auto" | "falcon" | "rest" | "embedded";

type AnyOpts = Parameters<typeof createCamundaClientBase>[0] & {
  config?: Record<string, unknown> & {
    CAMUNDA_TRANSPORT?: NanoTransport;
    CAMUNDA_REST_ADDRESS?: string;
    CAMUNDA_NANO_SUBMIT_TIMEOUT_MS?: string | number;
    /**
     * Force plain REST even when the gateway advertises Falcon. Useful for
     * environments where WebSockets are blocked (corporate proxies etc.).
     * Accepts any truthy string (`1`, `true`, `yes`, `on`).
     */
    CAMUNDA_FORCE_REST?: string | boolean | number;
  };
  /** Embedded (ADR 0005) in-process engine host; required when transport is "embedded". */
  embeddedHost?: EmbeddedHost;
};

function isTruthyEnv(v: unknown): boolean {
  if (v === undefined || v === null) return false;
  const s = String(v).trim().toLowerCase();
  return s !== "" && s !== "0" && s !== "off" && s !== "false" && s !== "no";
}

function forceRest(opts?: AnyOpts): boolean {
  const fromOpts = (opts?.config as any)?.CAMUNDA_FORCE_REST;
  const fromEnv =
    typeof process !== "undefined" ? process.env?.CAMUNDA_FORCE_REST : undefined;
  return isTruthyEnv(fromOpts) || isTruthyEnv(fromEnv);
}

function resolveMode(opts?: AnyOpts): NanoTransport {
  // Explicit escape hatch for environments where WebSockets are blocked
  // (e.g. corporate proxies). Wins over CAMUNDA_TRANSPORT.
  if (forceRest(opts)) return "rest";
  const fromOpts = opts?.config?.CAMUNDA_TRANSPORT as NanoTransport | undefined;
  const fromEnv = (typeof process !== "undefined" ? process.env?.CAMUNDA_TRANSPORT : undefined) as NanoTransport | undefined;
  return fromOpts ?? fromEnv ?? "auto";
}

/**
 * Default client-side submission-timeout (ms) for falcon creates, from
 * `opts.config.CAMUNDA_NANO_SUBMIT_TIMEOUT_MS` or the env var. `undefined`/`0`
 * means wait indefinitely under backpressure. A per-call `submitTimeoutMs` on
 * `createProcessInstance` input overrides this.
 */
function resolveSubmitTimeoutMs(opts?: AnyOpts): number | undefined {
  const raw =
    (opts?.config?.CAMUNDA_NANO_SUBMIT_TIMEOUT_MS as string | number | undefined) ??
    (typeof process !== "undefined" ? process.env?.CAMUNDA_NANO_SUBMIT_TIMEOUT_MS : undefined);
  const n = typeof raw === "string" ? Number(raw) : raw;
  return typeof n === "number" && Number.isFinite(n) && n > 0 ? n : undefined;
}

/** restAddress is normalized by the SDK to end with /v2; strip it for base. */
function baseFrom(restAddress: string): string {
  return normalizeBase(restAddress).replace(/\/v2$/, "");
}

/**
 * Drop-in replacement for the upstream createCamundaClient. Returns the upstream
 * client wrapped in a Proxy that upgrades createProcessInstance + createJobWorker
 * to the Falcon protocol when connected to a Nano server (overridable via the
 * CAMUNDA_TRANSPORT config: "auto" | "falcon" | "rest").
 */
export function createCamundaClient(opts?: AnyOpts): ReturnType<typeof createCamundaClientBase> {
  const client = createCamundaClientBase(opts as any);
  const mode = resolveMode(opts);
  if (mode === "rest") return client;

  // Embedded (ADR 0005): bind the in-process μ-nano host directly — no detection,
  // no socket. The host is the loopback "Nano gateway in the same process".
  if (mode === "embedded") {
    if (!opts?.embeddedHost) throw new Error("transport 'embedded' requires opts.embeddedHost");
    const transport = new EmbeddedTransport(opts.embeddedHost);
    return wrapClient(client, async () => transport as any, () => transport.close());
  }

  const restAddress = client.getConfig().restAddress;
  const base = baseFrom(restAddress);
  const submitTimeoutMs = resolveSubmitTimeoutMs(opts);

  let transport: FalconTransport | null = null;
  let nano: NanoInfo | null | undefined; // undefined = not yet probed
  // Sticky "falcon is not reachable" flag. Once we've seen a WebSocket handshake
  // fail (e.g. a proxy blocks WS), stop retrying for this client's lifetime and
  // fall back to REST — matching the escape-hatch semantics of CAMUNDA_FORCE_REST.
  let falconDead = false;
  let warnedDead = false;
  const markDead = (err: unknown) => {
    falconDead = true;
    transport = null;
    if (!warnedDead) {
      warnedDead = true;
      // eslint-disable-next-line no-console
      console.warn(
        `[@nanobpm/sdk] Falcon transport unavailable (${(err as Error)?.message ?? err}); ` +
          `falling back to REST. Set CAMUNDA_FORCE_REST=1 to skip Falcon detection.`,
      );
    }
  };

  const ensure = async (): Promise<FalconTransport | null> => {
    if (falconDead) return null;
    if (nano === undefined) {
      nano = mode === "falcon"
        ? { engine: "nanobpmn", falconPath: "/falcon" }
        : await detectNano(base);
    }
    if (!nano) return null;
    if (!transport) {
      const t = new FalconTransport(base, nano.falconPath, submitTimeoutMs);
      try {
        // Eagerly open the socket so a proxy-blocked WebSocket surfaces here
        // (single failure) rather than on every request.
        await t.connect();
        transport = t;
      } catch (e) {
        try { t.close(); } catch { /* ignore */ }
        markDead(e);
        return null;
      }
    }
    return transport;
  };
  return wrapClient(client, ensure, () => transport?.close());
}

/** Wrap an upstream client, upgrading createProcessInstance + createJobWorker to
 *  a Nano transport (falcon or embedded) when `ensure` yields one. */
function wrapClient(
  client: ReturnType<typeof createCamundaClientBase>,
  ensure: () => Promise<{ createInstance: Function; subscribe: Function; close: Function } | null>,
  onStop?: () => void,
): ReturnType<typeof createCamundaClientBase> {
  return new Proxy(client, {
    get(target, prop, receiver) {
      if (prop === "createProcessInstance") {
        return (input: any, options?: any) => {
          return ensure().then(async (t) => {
            if (!t) return (target as any).createProcessInstance(input, options);
            try {
              const r = await (t as any).createInstance({
                processDefinitionId: input?.processDefinitionId,
                processDefinitionKey: input?.processDefinitionKey,
                variables: input?.variables,
                awaitCompletion: input?.awaitCompletion ?? false,
                fetchVariables: input?.fetchVariables,
                submitTimeoutMs: input?.submitTimeoutMs,
              });
              if (r.status >= 400) throw new Error(`createInstance failed: ${r.status} ${JSON.stringify(r.body)}`);
              const body: any = r.body ?? {};
              return r.completion ? { ...body, variables: r.completion.variables, processInstanceKey: r.completion.processInstanceKey } : body;
            } catch (e) {
              // A late WS failure (proxy severed the socket, reconnect denied) —
              // fall back to REST for this call.
              const msg = String((e as Error)?.message ?? e);
              if (msg.includes("falcon closed") || msg.includes("falcon connect failed")) {
                // eslint-disable-next-line no-console
                console.warn(`[@nanobpm/sdk] Falcon call failed (${msg}); retrying via REST.`);
                return (target as any).createProcessInstance(input, options);
              }
              throw e;
            }
          });
        };
      }
      if (prop === "createJobWorker") {
        return (cfg: any) => {
          const w = new NanoJobWorker(null as any, { ...cfg, autoStart: false });
          void ensure().then(async (t) => {
            if (!t) { (target as any).createJobWorker(cfg); return; }
            w.bindTransport(t as any);
            try { await w.start(); }
            catch (e) {
              // eslint-disable-next-line no-console
              console.warn(
                `[@nanobpm/sdk] Falcon subscribe failed (${(e as Error)?.message ?? e}); ` +
                  `falling back to REST job worker.`,
              );
              (target as any).createJobWorker(cfg);
            }
          });
          return w as any;
        };
      }
      if (prop === "stopAllWorkers") {
        return () => { onStop?.(); return (target as any).stopAllWorkers(); };
      }
      return Reflect.get(target, prop, receiver);
    },
  });
}

export default createCamundaClient;
