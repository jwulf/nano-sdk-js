# @nanobpm/nano-sdk

A **drop-in replacement** for [`@camunda8/orchestration-cluster-api`](https://www.npmjs.com/package/@camunda8/orchestration-cluster-api) that transparently upgrades to Nano's **command-stream** protocol when connected to a [Nano](../nanobpmn) server. Against stock Camunda 8 it behaves exactly like the upstream SDK.

```diff
- import { createCamundaClient } from "@camunda8/orchestration-cluster-api";
+ import { createCamundaClient } from "@nanobpm/nano-sdk";
```

Everything else is re-exported unchanged — same types, same client, same API.

## What gets upgraded

When the client detects a Nano server, two hot paths move from REST onto a single persistent WebSocket (`/command-stream`):

- **`createProcessInstance`** → `createInstance` frame, gated on the server's submission-credit window (with `awaitCompletion` resolved via `instanceCompleted`).
- **`createJobWorker`** → `subscribe` + pushed `job` frames, acked with `completeJob`/`failJob`/`throwError` and credit-replenished.

This avoids the ~125 creates/s/conn ceiling of the REST long-poll path.

## Detection & override

Detection is automatic: the SDK probes `GET /v2/topology` once and looks for the Nano `commandStreamPath` advertisement. Control it via the `CAMUNDA_TRANSPORT` config (or env var):

| value            | behaviour                                   |
| ---------------- | ------------------------------------------- |
| `auto` (default) | command-stream on Nano, REST elsewhere      |
| `command-stream` | force the stream (assume Nano)              |
| `rest`           | never upgrade — pure upstream behaviour     |

```ts
const client = createCamundaClient({
  config: { CAMUNDA_REST_ADDRESS: "http://localhost:8080", CAMUNDA_TRANSPORT: "auto" },
});
```

## Failing fast under backpressure (`submitTimeoutMs`)

On the command stream, admission backpressure is expressed by the server
withholding submission credits (no `503`, no retry), so a `createProcessInstance`
otherwise waits indefinitely for the gateway to ack. To fail fast instead, set a
submit timeout — per call, or client-wide via config/env:

```ts
// Client-wide default (ms); per-call submitTimeoutMs wins when both are set.
const client = createCamundaClient({
  config: { CAMUNDA_REST_ADDRESS: "http://localhost:8080", CAMUNDA_NANO_SUBMIT_TIMEOUT_MS: 2000 },
});

import { SubmissionTimeoutError } from "@nanobpm/nano-sdk";

try {
  await client.createProcessInstance({ processDefinitionId: "order", submitTimeoutMs: 500 });
} catch (err) {
  if (err instanceof SubmissionTimeoutError) {
    // Server is applying admission backpressure — back off, do NOT tight-loop retry.
  }
}
```

`submitTimeoutMs` is a **client-side** guard: the SDK gates each create on the
server's submission-credit window (queuing client-side under backpressure rather
than flooding the gateway), and this bounds how long it waits for a credit before
rejecting with `SubmissionTimeoutError`. It is never sent on the wire. On timeout
the queued create is dropped so no credit slot leaks. Omit it (the default) to
wait indefinitely.

## Build & verify

```sh
npm install
npm run build
npm run verify   # against a running Nano server on :8080
```

`upstream-ref/` holds the cloned upstream SDK source for reference; the published package is the thin wrapper in `src/`.
