// Internal helpers for the pure-REST job-worker long-poll default.
//
// These are intentionally NOT re-exported from the package entrypoint: they are
// implementation details of the REST path, exercised directly by the unit tests
// (which import this module). Keeping them here keeps the public API surface
// focused on the SDK it re-exports and avoids leaking helper types into the
// published `.d.ts`.
import type { createCamundaClient as createCamundaClientBase } from "@camunda8/orchestration-cluster-api";

/** The `createJobWorker` config accepted by the upstream client, derived from
 *  the SDK entrypoint so we keep its full job-worker config type (including
 *  `pollTimeoutMs`) without importing extra types. */
export type RestJobWorkerConfig = Parameters<
  ReturnType<typeof createCamundaClientBase>["createJobWorker"]
>[0];

/**
 * Default broker long-poll window (ms) applied to a REST job worker when the
 * caller doesn't set one. Without it the upstream REST worker activates jobs
 * with `pollTimeoutMs` unset, so the Nano gateway falls back to its own short
 * default window (~5s) and an idle worker reconnects every few seconds. A 30s
 * window keeps an idle REST worker on one held connection ~6x longer, cutting
 * reconnect churn (and the chances of a transient connect failure) while the
 * broker still returns immediately the moment a job arrives. Falcon workers are
 * push-based and are unaffected. An explicit `pollTimeoutMs` (including
 * `0`/negative) always wins.
 */
export const REST_DEFAULT_POLL_TIMEOUT_MS = 30_000;

/** Inject the default REST long-poll window unless the caller set one. */
export function withRestPollDefault(cfg: RestJobWorkerConfig): RestJobWorkerConfig {
  if (cfg && cfg.pollTimeoutMs === undefined) {
    return { ...cfg, pollTimeoutMs: REST_DEFAULT_POLL_TIMEOUT_MS };
  }
  return cfg;
}

/** Wrap a base client so REST `createJobWorker` gets the default long-poll
 *  window, leaving every other method untouched. Used on the pure-REST path. */
export function wrapRestPollDefault(
  client: ReturnType<typeof createCamundaClientBase>,
): ReturnType<typeof createCamundaClientBase> {
  // Cache bound methods per property key so repeated reads return the same
  // function reference (stable identity), rather than allocating a fresh bound
  // function on every access.
  const boundCache = new Map<PropertyKey, unknown>();
  return new Proxy(client, {
    get(target, prop, receiver) {
      if (prop === "createJobWorker") {
        let wrapper = boundCache.get(prop);
        if (wrapper === undefined) {
          wrapper = (cfg: RestJobWorkerConfig) =>
            (target as any).createJobWorker(withRestPollDefault(cfg));
          boundCache.set(prop, wrapper);
        }
        return wrapper;
      }
      // Preserve original call semantics: bind forwarded methods to the
      // underlying client so `this` is the real client (not this Proxy),
      // which matters for methods that touch private fields.
      const value = Reflect.get(target, prop, receiver);
      if (typeof value !== "function") {
        return value;
      }
      let bound = boundCache.get(prop);
      if (bound === undefined) {
        bound = value.bind(target);
        boundCache.set(prop, bound);
      }
      return bound;
    },
  });
}
