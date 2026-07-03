// Embedded transport (ADR 0005, realization (a) "in-process direct"). Satisfies
// the same operation seam the falcon transport does — createInstance,
// subscribe/job-push, complete/fail/throw — but calls an in-process EmbeddedHost
// directly: no sockets, no frames. The host wraps μ-nano.wasm and owns the wall
// clock, durable journal, and timer/dispatch tick. Bind it via
// createCamundaClient({ config: { CAMUNDA_TRANSPORT: "embedded" }, embeddedHost }).

/** A job handed to a worker handler. */
export interface EmbeddedJob {
  jobKey: string;
  type: string;
  processInstanceKey: string;
  elementId: string;
  retries: number;
  variables: Record<string, unknown>;
}

/** The engine bridge an embedded app supplies (implemented by the app template's
 *  Deno host around μ-nano.wasm). Engine-core stays clock-free; the host injects now(). */
export interface EmbeddedHost {
  deploy(xml: string): Promise<{ processIds: string[] }>;
  createInstance(input: { processDefinitionId?: string; variables?: Record<string, unknown> }): Promise<{ processInstanceKey: string }>;
  activateJobs(type: string, max: number, timeoutMs: number, worker: string): Promise<EmbeddedJob[]>;
  completeJob(jobKey: string, variables?: Record<string, unknown>): Promise<void>;
  failJob(jobKey: string, retries: number, errorMessage?: string): Promise<void>;
  instanceCompleted(key: string): boolean;
  instanceVariables(key: string): Record<string, unknown>;
  /** Drive timers + dispatch at wall-clock now. Returns true if anything changed. */
  tick(): void;
}

interface Sub {
  worker: string;
  credits: number;
  timeoutMs: number;
  onJob: (j: EmbeddedJob) => void;
}

/** Pull-based dispatch over an in-process host. Structurally compatible with the
 *  FalconTransport surface used by NanoJobWorker + the client proxy. */
export class EmbeddedTransport {
  private subs = new Map<string, Sub>();
  private timer: ReturnType<typeof setInterval> | null = null;
  private closed = false;
  constructor(private host: EmbeddedHost, private pollMs = 25) {}

  async createInstance(input: {
    processDefinitionId?: string;
    variables?: Record<string, unknown>;
    awaitCompletion?: boolean;
  }): Promise<{ status: number; body: unknown; completion?: { processCompleted: boolean; variables: unknown; processInstanceKey: string } }> {
    const { processInstanceKey } = await this.host.createInstance(input);
    this.host.tick();
    if (!input.awaitCompletion) return { status: 200, body: { processInstanceKey } };
    while (!this.host.instanceCompleted(processInstanceKey)) {
      this.host.tick();
      await new Promise((r) => setTimeout(r, this.pollMs));
    }
    const variables = this.host.instanceVariables(processInstanceKey);
    return { status: 200, body: { processInstanceKey, variables }, completion: { processCompleted: true, variables, processInstanceKey } };
  }

  async subscribe(sub: { jobType: string; worker: string; credits: number; timeoutMs: number; onJob: (j: any) => void }): Promise<void> {
    this.subs.set(sub.jobType, { worker: sub.worker, credits: sub.credits, timeoutMs: sub.timeoutMs, onJob: sub.onJob });
    if (!this.timer) this.timer = setInterval(() => this.pump(), this.pollMs);
  }
  unsubscribe(jobType: string): void { this.subs.delete(jobType); }

  completeJob(jobKey: string, variables?: Record<string, unknown>): void { void this.host.completeJob(jobKey, variables); }
  failJob(jobKey: string, retries?: number, errorMessage?: string): void { void this.host.failJob(jobKey, retries ?? 0, errorMessage); }
  throwError(jobKey: string, _errorCode: string, errorMessage?: string): void { void this.host.failJob(jobKey, 0, errorMessage); }
  grantCredits(jobType: string, n: number): void { const s = this.subs.get(jobType); if (s) s.credits += n; }

  private pump(): void {
    if (this.closed) return;
    this.host.tick();
    for (const [type, s] of this.subs) {
      if (s.credits <= 0) continue;
      void this.host.activateJobs(type, s.credits, s.timeoutMs, s.worker).then((jobs) => {
        for (const j of jobs) { if (s.credits <= 0) break; s.credits--; s.onJob(j); }
      });
    }
  }
  close(): void { this.closed = true; if (this.timer) clearInterval(this.timer); }
}
