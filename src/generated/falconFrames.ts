/**
 * GENERATED — DO NOT EDIT.
 *
 * TypeScript frame types for the Falcon WebSocket protocol, generated from
 * `spec/falcon.asyncapi.yaml` (source: nanobpmn `docs/falcon.asyncapi.yaml`)
 * by `scripts/generate-falcon.mjs`. Regenerate with `npm run generate`.
 *
 * Spec content hash: ae4e317768f8
 */
/* eslint-disable */

export type AnyFalconFrame =
  | Subscribe
  | JobCredits
  | CreateInstance
  | CompleteJob
  | FailJob
  | ThrowError
  | AwaitInstance
  | Heartbeat
  | Welcome
  | Job
  | CommandResult
  | InstanceCompleted
  | SubmissionCredits
  | Pressure
  | WorkerAdvice;
/**
 * Client-chosen correlation id. The server echoes it on the matching `commandResult` and on the `instanceCompleted` for an awaited create.
 *
 */
export type Corr = number;
/**
 * A 64-bit nanobpmn key serialized as a decimal string (jobKey, processInstanceKey, ...).
 *
 */
export type Key = string;

export interface Subscribe {
  type: "subscribe";
  /**
   * The BPMN job type to receive.
   */
  jobType: string;
  /**
   * Initial job-delivery demand for this type.
   */
  jobCredits?: number;
  /**
   * Restrict fetched variables to these names (null = all).
   */
  fetchVariable?: string[];
  /**
   * Job activation lock (in milliseconds) applied to every job pushed to this subscription. A null or non-positive value is treated as unset and the server applies a 60000 ms default — a zero lock would make each leased job instantly re-activatable and cause duplicate delivery before the worker completes it.
   */
  timeout?: number;
  /**
   * Worker name recorded on activation.
   */
  worker?: string;
}
export interface JobCredits {
  type: "jobCredits";
  jobType: string;
  /**
   * Additional job-delivery credits to grant for this type.
   */
  n: number;
}
export interface CreateInstance {
  type: "createInstance";
  corr: Corr;
  /**
   * BPMN process id (one of id/key is required).
   */
  processDefinitionId?: string;
  /**
   * Numeric process definition key as a string.
   */
  processDefinitionKey?: string;
  /**
   * Initial process variables.
   */
  variables?: {
    [k: string]: unknown;
  };
  /**
   * If true, the server emits an `instanceCompleted` frame (correlated by `corr`) when the instance reaches a terminal state, in addition to the immediate `commandResult` ack.
   *
   */
  awaitCompletion?: boolean;
  /**
   * Variables to include in the `instanceCompleted` payload.
   */
  fetchVariables?: string[];
  /**
   * Await timeout in milliseconds.
   */
  requestTimeout?: number;
}
export interface CompleteJob {
  type: "completeJob";
  corr: Corr;
  jobKey: Key;
  variables?: {
    [k: string]: unknown;
  };
}
export interface FailJob {
  type: "failJob";
  corr: Corr;
  jobKey: Key;
  retries?: number;
  errorMessage?: string;
}
export interface ThrowError {
  type: "throwError";
  corr: Corr;
  jobKey: Key;
  errorCode: string;
  errorMessage?: string;
}
export interface AwaitInstance {
  type: "awaitInstance";
  corr: Corr;
  processInstanceKey: Key;
  fetchVariables?: string[];
  requestTimeout?: number;
}
export interface Heartbeat {
  type: "heartbeat";
}
export interface Welcome {
  type: "welcome";
  /**
   * Initial create-side submission window for this connection.
   */
  submissionCredits: number;
  /**
   * Server heartbeat cadence in milliseconds.
   */
  heartbeatMs: number;
}
export interface Job {
  type: "job";
  /**
   * An activated job, same shape as REST `ActivatedJobResult`.
   */
  job: {
    [k: string]: unknown;
  };
}
export interface CommandResult {
  type: "commandResult";
  corr: Corr;
  /**
   * HTTP-equivalent status code for the command (e.g. 200, 404).
   */
  status: number;
  /**
   * Result payload (e.g. the create result with `processInstanceKey`) or an error body. Omitted when there is no body.
   *
   */
  body?: {
    [k: string]: unknown;
  };
}
export interface InstanceCompleted {
  type: "instanceCompleted";
  corr: Corr;
  processInstanceKey: Key;
  /**
   * True if the instance completed normally; false if it was terminated.
   *
   */
  processCompleted: boolean;
  /**
   * Fetched output variables (object), or empty if none requested.
   */
  variables: {
    [k: string]: unknown;
  };
}
export interface SubmissionCredits {
  type: "submissionCredits";
  /**
   * Additional submission credits granted.
   */
  n: number;
}
export interface Pressure {
  type: "pressure";
  /**
   * Coarse pressure level (e.g. "ok", "high").
   */
  level: string;
  /**
   * Suggested backoff before resubmitting, if applicable.
   */
  retryAfterMs?: number;
}
export interface WorkerAdvice {
  type: "workerAdvice";
  /**
   * Recommended active dispatch width per worker. A cooperating SDK can park subscribers down toward this target. `0` = no advice (governor disabled) and is never broadcast.
   *
   */
  recommendedConcurrency: number;
}

/** Every frame a public client may send on `/falcon` (sendCommand operation). */
export type ClientFrame =
  | Subscribe
  | JobCredits
  | CreateInstance
  | CompleteJob
  | FailJob
  | ThrowError
  | AwaitInstance
  | Heartbeat;

/** Every frame the server may push to a client (receiveEvent operation). */
export type ServerFrame =
  | Welcome
  | Job
  | CommandResult
  | InstanceCompleted
  | SubmissionCredits
  | Pressure
  | WorkerAdvice
  | Heartbeat;

/** Discriminator (`type`) tags for {@link ClientFrame}. */
export type ClientFrameType = "subscribe" | "jobCredits" | "createInstance" | "completeJob" | "failJob" | "throwError" | "awaitInstance" | "heartbeat";

/** Discriminator (`type`) tags for {@link ServerFrame}. */
export type ServerFrameType = "welcome" | "job" | "commandResult" | "instanceCompleted" | "submissionCredits" | "pressure" | "workerAdvice" | "heartbeat";
