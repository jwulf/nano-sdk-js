// A drop-in JobWorker that drains jobs over the command stream. Mirrors the
// upstream JobWorker surface (close/stop/start) but receives pushed jobs instead
// of long-polling. Each handled job replenishes one credit, keeping demand at
// maxParallelJobs.
import { JobActionReceiptSymbol as JobActionReceipt } from "@camunda8/orchestration-cluster-api";
import type { CommandStreamTransport, JobFrame } from "./transport.js";

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
  private transport: CommandStreamTransport;
  private cfg: NanoJobWorkerConfig;
  private credits: number;
  private stopped = false;
  readonly jobType: string;
  readonly name: string;

  constructor(transport: CommandStreamTransport, cfg: NanoJobWorkerConfig) {
    this.transport = transport;
    this.cfg = cfg;
    this.jobType = cfg.jobType;
    this.credits = cfg.maxParallelJobs ?? 10;
    this.name = cfg.workerName || `worker-${cfg.jobType}`;
    if (cfg.autoStart !== false) void this.start();
  }

  /** Bind the transport after async Nano detection. */
  bindTransport(transport: CommandStreamTransport): void {
    this.transport = transport;
  }

  async start(): Promise<void> {
    this.stopped = false;
    await this.transport.subscribe({
      jobType: this.jobType,
      worker: this.name,
      credits: this.credits,
      timeoutMs: this.cfg.jobTimeoutMs ?? 60_000,
      fetchVariables: this.cfg.fetchVariables ?? null,
      onJob: (raw) => void this.dispatch(raw),
    });
  }

  private enrich(raw: JobFrame) {
    let acted = false;
    const ack = (replenish = true) => {
      if (!acted) {
        acted = true;
        if (replenish) this.transport.grantCredits(this.jobType, 1);
      }
    };
    return {
      ...raw,
      complete: async (variables: Record<string, unknown> = {}) => {
        this.transport.completeJob(raw.jobKey, variables);
        ack();
        return JobActionReceipt;
      },
      fail: async (reason: { errorMessage?: string; retries?: number } = {}) => {
        this.transport.failJob(raw.jobKey, reason.retries, reason.errorMessage);
        ack();
        return JobActionReceipt;
      },
      error: async (e: { errorCode: string; errorMessage?: string }) => {
        this.transport.throwError(raw.jobKey, e.errorCode, e.errorMessage);
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
        this.transport.failJob(raw.jobKey, 0, String(err));
      } finally {
        this.transport.grantCredits(this.jobType, 1);
      }
    }
  }

  stop(): void {
    this.stopped = true;
    this.transport.unsubscribe(this.jobType);
  }
  close(): void {
    this.stop();
  }
}
