// @nanobpm/nano-sdk — a drop-in replacement for @camunda8/orchestration-cluster-api.
//
// It re-exports the entire upstream SDK surface, then overrides
// `createCamundaClient` (and the default export) so that, when connected to a
// Nano server, process-instance creation and job workers transparently upgrade
// to Nano's command-stream protocol. Against stock Camunda 8 it is a no-op: the
// upstream REST behaviour is used unchanged.
import {
  createCamundaClient as createCamundaClientBase,
} from "@camunda8/orchestration-cluster-api";
import { detectNano, normalizeBase, type NanoInfo } from "./detect.js";
import { CommandStreamTransport } from "./transport.js";
import { NanoJobWorker } from "./nanoWorker.js";

// Re-export everything else so consumers can swap the import path and nothing
// breaks.
export * from "@camunda8/orchestration-cluster-api";
export { detectNano, type NanoInfo } from "./detect.js";
export { CommandStreamTransport } from "./transport.js";
export { NanoJobWorker } from "./nanoWorker.js";

/** auto: upgrade only on Nano. command-stream: force. rest: never upgrade. */
export type NanoTransport = "auto" | "command-stream" | "rest";

type AnyOpts = Parameters<typeof createCamundaClientBase>[0] & {
  config?: Record<string, unknown> & { CAMUNDA_TRANSPORT?: NanoTransport; CAMUNDA_REST_ADDRESS?: string };
};

function resolveMode(opts?: AnyOpts): NanoTransport {
  const fromOpts = opts?.config?.CAMUNDA_TRANSPORT as NanoTransport | undefined;
  const fromEnv = (typeof process !== "undefined" ? process.env?.CAMUNDA_TRANSPORT : undefined) as NanoTransport | undefined;
  return fromOpts ?? fromEnv ?? "auto";
}

/** restAddress is normalized by the SDK to end with /v2; strip it for base. */
function baseFrom(restAddress: string): string {
  return normalizeBase(restAddress).replace(/\/v2$/, "");
}

/**
 * Drop-in replacement for the upstream createCamundaClient. Returns the upstream
 * client wrapped in a Proxy that upgrades createProcessInstance + createJobWorker
 * to the command stream when connected to a Nano server (overridable via the
 * CAMUNDA_TRANSPORT config: "auto" | "command-stream" | "rest").
 */
export function createCamundaClient(opts?: AnyOpts): ReturnType<typeof createCamundaClientBase> {
  const client = createCamundaClientBase(opts as any);
  const mode = resolveMode(opts);
  if (mode === "rest") return client;

  const restAddress = client.getConfig().restAddress;
  const base = baseFrom(restAddress);

  let transport: CommandStreamTransport | null = null;
  let nano: NanoInfo | null | undefined; // undefined = not yet probed

  const ensure = async (): Promise<CommandStreamTransport | null> => {
    if (nano === undefined) {
      nano = mode === "command-stream"
        ? { engine: "nanobpmn", commandStreamPath: "/command-stream" }
        : await detectNano(base);
    }
    if (!nano) return null;
    if (!transport) transport = new CommandStreamTransport(base, nano.commandStreamPath);
    return transport;
  };

  return new Proxy(client, {
    get(target, prop, receiver) {
      if (prop === "createProcessInstance") {
        return (input: any, options?: any) => {
          const useStream = ensure();
          return useStream.then(async (t) => {
            if (!t) return (target as any).createProcessInstance(input, options);
            const r = await t.createInstance({
              processDefinitionId: input?.processDefinitionId,
              processDefinitionKey: input?.processDefinitionKey,
              variables: input?.variables,
              awaitCompletion: input?.awaitCompletion ?? false,
              fetchVariables: input?.fetchVariables,
            });
            if (r.status >= 400) throw new Error(`createInstance failed: ${r.status} ${JSON.stringify(r.body)}`);
            const body: any = r.body ?? {};
            return r.completion ? { ...body, variables: r.completion.variables, processInstanceKey: r.completion.processInstanceKey } : body;
          });
        };
      }
      if (prop === "createJobWorker") {
        return (cfg: any) => {
          const w = new NanoJobWorker(null as any, { ...cfg, autoStart: false });
          void ensure().then((t) => {
            if (t) { w.bindTransport(t); void w.start(); }
            else (target as any).createJobWorker(cfg);
          });
          return w as any;
        };
      }
      if (prop === "stopAllWorkers") {
        return () => {
          transport?.close();
          return (target as any).stopAllWorkers();
        };
      }
      return Reflect.get(target, prop, receiver);
    },
  });
}

export default createCamundaClient;
