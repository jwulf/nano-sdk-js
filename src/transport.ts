// Command-stream transport. One persistent WebSocket per client multiplexes:
//   * createInstance (corr-correlated CommandResult, awaitCompletion via
//     InstanceCompleted), and
//   * job subscriptions (welcome -> subscribe -> job push, replenished by
//     jobCredits; completeJob/failJob/throwError as unmetered drains).
// Mirrors the protocol implemented by the engine (server/src/command_stream.rs)
// and the embedded worker SDK (server/src/console/worker_sdk.ts).
import { commandStreamUrl } from "./detect.js";
import { getWebSocket } from "./ws.js";

type Json = Record<string, unknown>;

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

export class CommandStreamTransport {
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

  constructor(restAddress: string, path: string) {
    this.url = commandStreamUrl(restAddress, path);
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
          if (!this.open) reject(new Error(`command-stream connect failed: ${this.url}`));
        };
        ws.onclose = () => {
          this.open = false;
          if (this.heartbeat) clearInterval(this.heartbeat);
          // Reject in-flight commands; resubscribe on reconnect.
          for (const p of this.pending.values()) p.reject(new Error("command-stream closed"));
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
        const hb = Number(f.heartbeatMs ?? 0);
        if (hb > 0) {
          if (this.heartbeat) clearInterval(this.heartbeat);
          this.heartbeat = setInterval(() => this.send({ type: "heartbeat" }), hb);
        }
        for (const sub of this.subs.values()) this.sendSubscribe(sub);
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
      // submissionCredits / pressure / heartbeat: no client action needed yet.
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

  /** Create a process instance over the stream. */
  async createInstance(input: {
    processDefinitionId?: string;
    processDefinitionKey?: string;
    variables?: Record<string, unknown>;
    awaitCompletion?: boolean;
    fetchVariables?: string[];
    requestTimeoutMs?: number;
  }): Promise<{ status: number; body: unknown; completion?: { processCompleted: boolean; variables: unknown; processInstanceKey: string } }> {
    await this.connect();
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
