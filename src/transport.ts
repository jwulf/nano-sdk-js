// Falcon transport. One persistent WebSocket per client multiplexes:
//   * createInstance (corr-correlated CommandResult, awaitCompletion via
//     InstanceCompleted), and
//   * job subscriptions (welcome -> subscribe -> job push, replenished by
//     jobCredits; completeJob/failJob/throwError as unmetered drains).
// Mirrors the protocol implemented by the engine (server/src/falcon.rs)
// and the embedded worker SDK (server/src/console/worker_sdk.ts).
import { falconUrl } from "./detect.js";
import type { ServerFrameType } from "./generated/falconFrames.js";
import { getWebSocket } from "./ws.js";

type Json = Record<string, unknown>;

/**
 * Compile-time drift guard: every documented server frame `type` maps to `true`
 * here. `ServerFrameType` is generated from the Falcon AsyncAPI spec, so after
 * regenerating (`npm run generate`) a newly documented server frame makes this
 * object literal error ("property missing") until it is handled in
 * {@link FalconTransport}'s `handle` switch (or explicitly parked as advisory).
 * Mirrors the server-side exhaustiveness tripwire in `server/src/falcon.rs`.
 */
const HANDLED_SERVER_FRAMES: Record<ServerFrameType, true> = {
  welcome: true,
  job: true,
  commandResult: true,
  instanceCompleted: true,
  submissionCredits: true,
  pressure: true,
  workerAdvice: true,
  heartbeat: true,
};

export interface JobFrame {
  jobKey: string;
  type: string;
  processInstanceKey: string;
  variables?: Record<string, unknown>;
  customHeaders?: Record<string, unknown>;
  retries?: number;
  [k: string]: unknown;
}

export interface Subscription {
  jobType: string;
  worker: string;
  credits: number;
  timeoutMs: number;
  fetchVariables: string[] | null;
  onJob: (job: JobFrame) => void;
}

interface Pending {
  resolve: (v: { status: number; body: unknown }) => void;
  reject: (e: unknown) => void;
}

/** A create queued behind an exhausted submission-credit window. */
interface CreditWaiter {
  /** Grant a credit (decrement + resolve the acquire). */
  grant: () => void;
  /** Abandon the wait (reject the acquire), e.g. on disconnect. */
  fail: (e: unknown) => void;
}

/**
 * Thrown by {@link FalconTransport.createInstance} when the gateway does
 * not acknowledge a create within `submitTimeoutMs`. On the Falcon protocol,
 * admission backpressure is expressed by the server withholding submission
 * credits (no `503`, no retry), so a create otherwise waits indefinitely for
 * intake capacity. This turns that stall into a typed rejection — treat it as
 * "the server is backpressured" and back off; do not tight-loop retry.
 */
export class SubmissionTimeoutError extends Error {
  constructor(readonly timeoutMs: number) {
    super(
      `create submission stalled: no gateway ack within ${timeoutMs}ms (server is applying admission backpressure)`,
    );
    this.name = "SubmissionTimeoutError";
  }
}

export class FalconTransport {
  private url: string;
  private ws: WebSocket | null = null;
  private open = false;
  private corr = 0;
  private pending = new Map<number, Pending>();
  private awaits = new Map<number, (v: { processCompleted: boolean; variables: unknown; processInstanceKey: string }) => void>();
  private subs = new Map<string, Subscription>();
  private heartbeat: ReturnType<typeof setInterval> | null = null;
  private connectPromise: Promise<void> | null = null;
  private closed = false;
  private defaultSubmitTimeoutMs?: number;
  /**
   * Server-granted submission-credit window (seeded by `welcome`, topped up by
   * `submissionCredits`). Mirrors the engine's intake metering so creates queue
   * client-side under admission backpressure instead of flooding the gateway.
   */
  private credits = 0;
  private creditWaiters: CreditWaiter[] = [];

  constructor(restAddress: string, path: string, defaultSubmitTimeoutMs?: number) {
    this.url = falconUrl(restAddress, path);
    this.defaultSubmitTimeoutMs =
      defaultSubmitTimeoutMs !== undefined && defaultSubmitTimeoutMs > 0
        ? defaultSubmitTimeoutMs
        : undefined;
  }

  private nextCorr(): number {
    this.corr += 1;
    return this.corr;
  }

  private send(frame: Json): void {
    if (this.ws && this.open) this.ws.send(JSON.stringify(frame));
  }

  async connect(): Promise<void> {
    if (this.open) return;
    if (this.connectPromise) return this.connectPromise;
    this.connectPromise = new Promise<void>((resolve, reject) => {
      void getWebSocket().then((WS) => {
        const ws = new WS(this.url);
        this.ws = ws;
        ws.onopen = () => {};
        ws.onmessage = (ev: MessageEvent) => {
          let f: Json;
          try {
            f = JSON.parse(typeof ev.data === "string" ? ev.data : String(ev.data));
          } catch {
            return;
          }
          this.handle(f, resolve);
        };
        ws.onerror = () => {
          if (!this.open) reject(new Error(`falcon connect failed: ${this.url}`));
        };
        ws.onclose = () => {
          this.open = false;
          // The credit window is per-connection; a reconnect's welcome re-grants
          // a fresh one. Reset it and fail any queued creates so callers can retry
          // on the new connection rather than hang against a dead window.
          this.credits = 0;
          const waiters = this.creditWaiters;
          this.creditWaiters = [];
          for (const w of waiters) w.fail(new Error("falcon closed"));
          if (this.heartbeat) clearInterval(this.heartbeat);
          // Reject in-flight commands; resubscribe on reconnect.
          for (const p of this.pending.values()) p.reject(new Error("falcon closed"));
          this.pending.clear();
          if (!this.closed) setTimeout(() => void this.reconnect(), 1000);
        };
      });
    });
    return this.connectPromise;
  }

  private async reconnect(): Promise<void> {
    this.connectPromise = null;
    this.open = false;
    await this.connect();
    for (const sub of this.subs.values()) this.sendSubscribe(sub);
  }

  private handle(f: Json, resolveConnect: () => void): void {
    switch (f.type) {
      case "welcome": {
        this.open = true;
        this.credits = Number(f.submissionCredits ?? 0);
        const hb = Number(f.heartbeatMs ?? 0);
        if (hb > 0) {
          if (this.heartbeat) clearInterval(this.heartbeat);
          this.heartbeat = setInterval(() => this.send({ type: "heartbeat" }), hb);
        }
        for (const sub of this.subs.values()) this.sendSubscribe(sub);
        this.releaseCreditWaiters();
        resolveConnect();
        break;
      }
      case "job": {
        const job = (f.job as JobFrame) ?? ({} as JobFrame);
        const sub = this.subs.get(job.type);
        if (sub) sub.onJob(job);
        break;
      }
      case "commandResult": {
        const corr = Number(f.corr ?? 0);
        const p = this.pending.get(corr);
        if (p) {
          this.pending.delete(corr);
          p.resolve({ status: Number(f.status ?? 0), body: f.body ?? null });
        }
        break;
      }
      case "instanceCompleted": {
        const corr = Number(f.corr ?? 0);
        const cb = this.awaits.get(corr);
        if (cb) {
          this.awaits.delete(corr);
          cb({
            processCompleted: Boolean(f.processCompleted),
            variables: f.variables ?? {},
            processInstanceKey: String(f.processInstanceKey ?? ""),
          });
        }
        break;
      }
      case "submissionCredits": {
        // Server topped up the create-side window (intake headroom returned).
        this.credits += Number(f.n ?? 0);
        this.releaseCreditWaiters();
        break;
      }
      case "pressure":
      case "workerAdvice":
      case "heartbeat":
        // Advisory server frames: no client action needed yet. `workerAdvice`
        // could later drive subscriber self-sizing; `pressure` a backoff hint.
        break;
      default:
        // Forward-compatible: silently ignore any frame this client version
        // does not recognise, so a newer server never breaks an older client.
        // (Known-but-advisory frames are handled above; this is the unknown
        // tail. `HANDLED_SERVER_FRAMES` is the spec-drift guard for the set.)
        void HANDLED_SERVER_FRAMES;
        break;
    }
  }

  private sendSubscribe(sub: Subscription): void {
    this.send({
      type: "subscribe",
      jobType: sub.jobType,
      jobCredits: sub.credits,
      worker: sub.worker,
      timeout: sub.timeoutMs,
      fetchVariable: sub.fetchVariables,
    });
  }

  // ----- submission-credit gating ------------------------------------------

  /**
   * Take one submission credit, waiting for the gateway to replenish when the
   * window is exhausted (admission backpressure — no 503, no retry). When
   * `timeoutMs` elapses first, rejects with {@link SubmissionTimeoutError} and
   * removes the queued waiter so no credit slot leaks.
   */
  private acquireCredit(timeoutMs?: number): Promise<void> {
    if (this.credits > 0) {
      this.credits -= 1;
      return Promise.resolve();
    }
    return new Promise<void>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const waiter: CreditWaiter = {
        grant: () => {
          if (timer !== undefined) clearTimeout(timer);
          this.credits -= 1;
          resolve();
        },
        fail: (e) => {
          if (timer !== undefined) clearTimeout(timer);
          reject(e);
        },
      };
      this.creditWaiters.push(waiter);
      if (timeoutMs !== undefined && timeoutMs > 0) {
        timer = setTimeout(() => {
          const i = this.creditWaiters.indexOf(waiter);
          if (i >= 0) this.creditWaiters.splice(i, 1);
          reject(new SubmissionTimeoutError(timeoutMs));
        }, timeoutMs);
        timer.unref?.();
      }
    });
  }

  private releaseCreditWaiters(): void {
    while (this.credits > 0 && this.creditWaiters.length > 0) {
      this.creditWaiters.shift()!.grant();
    }
  }

  /** Create a process instance over the stream. */
  async createInstance(input: {
    processDefinitionId?: string;
    processDefinitionKey?: string;
    variables?: Record<string, unknown>;
    awaitCompletion?: boolean;
    fetchVariables?: string[];
    requestTimeoutMs?: number;
    /**
     * Client-side bound (ms) on how long to wait for a submission credit before
     * rejecting with {@link SubmissionTimeoutError}. Overrides the transport-wide
     * default. Omit to wait indefinitely under backpressure. Never sent on the
     * wire — the server is unaware of it.
     */
    submitTimeoutMs?: number;
  }): Promise<{ status: number; body: unknown; completion?: { processCompleted: boolean; variables: unknown; processInstanceKey: string } }> {
    await this.connect();
    // Gate intake on the server's submission-credit window: block here (bounded
    // by submitTimeoutMs) rather than flooding the gateway with creates it has
    // not granted capacity for. Job completions stay unmetered.
    await this.acquireCredit(input.submitTimeoutMs ?? this.defaultSubmitTimeoutMs);
    const corr = this.nextCorr();
    let completionResolve: ((v: { processCompleted: boolean; variables: unknown; processInstanceKey: string }) => void) | null = null;
    const completion = input.awaitCompletion
      ? new Promise<{ processCompleted: boolean; variables: unknown; processInstanceKey: string }>((r) => (completionResolve = r))
      : null;
    if (completionResolve) this.awaits.set(corr, completionResolve);
    const result = await new Promise<{ status: number; body: unknown }>((resolve, reject) => {
      this.pending.set(corr, { resolve, reject });
      this.send({
        type: "createInstance",
        corr,
        processDefinitionId: input.processDefinitionId ?? null,
        processDefinitionKey: input.processDefinitionKey ?? null,
        variables: input.variables ?? null,
        awaitCompletion: input.awaitCompletion ?? false,
        fetchVariables: input.fetchVariables ?? null,
        requestTimeout: input.requestTimeoutMs ?? null,
      });
    });
    const done = completion ? await completion : undefined;
    return { ...result, completion: done };
  }

  /** Subscribe a worker; jobs arrive via sub.onJob, credits replenished by ack helpers. */
  async subscribe(sub: Subscription): Promise<void> {
    this.subs.set(sub.jobType, sub);
    await this.connect();
    this.sendSubscribe(sub);
  }

  unsubscribe(jobType: string): void {
    this.subs.delete(jobType);
  }

  completeJob(jobKey: string, variables?: Record<string, unknown>): void {
    this.send({ type: "completeJob", corr: this.nextCorr(), jobKey, variables: variables ?? null });
  }
  failJob(jobKey: string, retries?: number, errorMessage?: string): void {
    this.send({ type: "failJob", corr: this.nextCorr(), jobKey, retries: retries ?? 0, errorMessage: errorMessage ?? null });
  }
  throwError(jobKey: string, errorCode: string, errorMessage?: string): void {
    this.send({ type: "throwError", corr: this.nextCorr(), jobKey, errorCode, errorMessage: errorMessage ?? null });
  }
  grantCredits(jobType: string, n: number): void {
    this.send({ type: "jobCredits", jobType, n });
  }

  close(): void {
    this.closed = true;
    if (this.heartbeat) clearInterval(this.heartbeat);
    try {
      this.ws?.close(1000, "shutdown");
    } catch {
      /* ignore */
    }
  }
}
