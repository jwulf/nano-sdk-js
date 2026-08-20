// A drop-in JobWorker that drains jobs over the Falcon protocol. Mirrors the
// upstream JobWorker surface (close/stop/start) but receives pushed jobs instead
// of long-polling. Each handled job replenishes one credit, keeping demand at
// maxParallelJobs.
import { JobActionReceiptSymbol as JobActionReceipt } from "@camunda8/orchestration-cluster-api";
import type { FalconTransport, JobFrame } from "./transport.js";

export interface NanoJobWorkerConfig {
  jobType: string;
  workerName?: string;
  maxParallelJobs?: number;
  jobTimeoutMs?: number;
  fetchVariables?: string[];
  jobHandler: (job: any) => Promise<typeof JobActionReceipt> | typeof JobActionReceipt;
  autoStart?: boolean;
}

export class NanoJobWorker {
  private transport: FalconTransport | null;
  private cfg: NanoJobWorkerConfig;
  private credits: number;
  private stopped = false;
  /** A start() was requested while transport was still null; replay it on bind. */
  private startRequested = false;
  /**
   * Shared in-flight (then settled) subscription attempt. Racing start() /
   * bindTransport() calls await this same promise so they subscribe at most
   * once, yet all observe a rejection if transport.subscribe() fails. Reset to
   * null on failure (so a later start() can retry) and on stop().
   */
  private subscribePromise: Promise<void> | null = null;
  readonly jobType: string;
  readonly name: string;

  constructor(transport: FalconTransport | null, cfg: NanoJobWorkerConfig) {
    this.transport = transport;
    this.cfg = cfg;
    this.jobType = cfg.jobType;
    this.credits = cfg.maxParallelJobs ?? 10;
    this.name = cfg.workerName || `worker-${cfg.jobType}`;
    if (cfg.autoStart !== false) void this.start();
  }

  /**
   * Bind the transport after async Nano detection. If a start() was requested
   * before the transport was available, honour it now (subscribing exactly once).
   */
  bindTransport(transport: FalconTransport): void {
    this.transport = transport;
    // Fire-and-forget replay: swallow the rejection on this path so it is not an
    // unhandled rejection. The shared subscribePromise still rejects, so any
    // start() awaiting it observes the failure (and resets for a later retry).
    if (this.startRequested && !this.stopped) void this.subscribe().catch(() => {});
  }

  /**
   * Begin draining jobs. Null-safe and idempotent:
   * - If the transport is not yet bound, the request is deferred and replayed by
   *   bindTransport() once it is — no null dereference.
   * - Duplicate calls (e.g. proxy self-start plus an eager caller start) result
   *   in a single subscription.
   */
  async start(): Promise<void> {
    this.stopped = false;
    this.startRequested = true;
    if (this.transport === null) return; // deferred until bindTransport()
    await this.subscribe();
  }

  private subscribe(): Promise<void> {
    if (this.stopped || this.transport === null) return Promise.resolve();
    // Share a single in-flight promise so racing callers coalesce onto one
    // attempt instead of relying on a synchronous flag set before the await.
    if (this.subscribePromise !== null) return this.subscribePromise;
    this.subscribePromise = this.transport
      .subscribe({
        jobType: this.jobType,
        worker: this.name,
        credits: this.credits,
        timeoutMs: this.cfg.jobTimeoutMs ?? 60_000,
        fetchVariables: this.cfg.fetchVariables ?? null,
        onJob: (raw) => void this.dispatch(raw),
      })
      .catch((err) => {
        // Do not wedge the worker as "subscribed" on failure: clear the shared
        // promise so a later start() can retry, and rethrow so awaiting callers
        // (including the proxy's fallback-to-REST catch) observe the error.
        this.subscribePromise = null;
        throw err;
      });
    return this.subscribePromise;
  }

  private enrich(raw: JobFrame) {
    let acted = false;
    const ack = (replenish = true) => {
      if (!acted) {
        acted = true;
        if (replenish) this.transport?.grantCredits(this.jobType, 1);
      }
    };
    return {
      ...raw,
      complete: async (variables: Record<string, unknown> = {}) => {
        this.transport?.completeJob(raw.jobKey, variables);
        ack();
        return JobActionReceipt;
      },
      fail: async (reason: { errorMessage?: string; retries?: number } = {}) => {
        this.transport?.failJob(raw.jobKey, reason.retries, reason.errorMessage);
        ack();
        return JobActionReceipt;
      },
      error: async (e: { errorCode: string; errorMessage?: string }) => {
        this.transport?.throwError(raw.jobKey, e.errorCode, e.errorMessage);
        ack();
        return JobActionReceipt;
      },
      ignore: async () => {
        ack();
        return JobActionReceipt;
      },
    };
  }

  private async dispatch(raw: JobFrame): Promise<void> {
    if (this.stopped) return;
    const job = this.enrich(raw);
    try {
      await this.cfg.jobHandler(job);
    } catch (err) {
      try {
        this.transport?.failJob(raw.jobKey, 0, String(err));
      } finally {
        this.transport?.grantCredits(this.jobType, 1);
      }
    }
  }

  stop(): void {
    this.stopped = true;
    this.startRequested = false;
    this.subscribePromise = null;
    this.transport?.unsubscribe(this.jobType);
  }
  close(): void {
    this.stop();
  }
}
