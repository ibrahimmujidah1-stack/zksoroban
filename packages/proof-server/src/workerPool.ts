import { Worker } from "node:worker_threads";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import type { ProofJobData, ProofJobResult, ProveSuccessResponse } from "./types.js";

interface PendingTask {
  job: ProofJobData;
  resolve: (res: ProveSuccessResponse) => void;
  reject: (err: Error) => void;
}

interface WorkerState {
  worker: Worker;
  busy: boolean;
  currentTaskId?: string;
}

export class WorkerPool {
  private workers: WorkerState[] = [];
  private taskQueue: PendingTask[] = [];
  private taskMap = new Map<string, PendingTask>();
  private closed = false;
  private readonly workerScript: string;

  constructor(
    public readonly poolSize: number = Math.max(2, os.cpus()?.length || 2),
    workerScriptPath?: string
  ) {
    this.workerScript =
      workerScriptPath || path.resolve(__dirname, "worker.js");

    for (let i = 0; i < this.poolSize; i++) {
      this.workers.push(this.createWorkerState(this.workerScript));
    }
  }

  private createWorkerState(script: string): WorkerState {
    const worker = new Worker(script);
    const workerState: WorkerState = {
      worker,
      busy: false,
    };

    worker.on("message", (msg: ProofJobResult) => {
      this.onWorkerMessage(workerState, msg);
    });

    worker.on("error", (err: unknown) => {
      const error = err instanceof Error ? err : new Error(String(err));
      this.onWorkerError(workerState, error);
    });

    worker.on("exit", (code: number) => {
      if (workerState.currentTaskId) {
        const task = this.taskMap.get(workerState.currentTaskId);
        if (task) {
          this.taskMap.delete(workerState.currentTaskId);
          task.reject(new Error(`Worker exited unexpectedly with code ${code}`));
        }
        workerState.busy = false;
        workerState.currentTaskId = undefined;
      }
      if (!this.closed) {
        // Restart worker if it crashed or exited unexpectedly
        this.restartWorker(workerState, script);
      }
    });

    return workerState;
  }

  public get size(): number {
    return this.workers.length;
  }

  public get queueLength(): number {
    return this.taskQueue.length;
  }

  public get busyWorkers(): number {
    return this.workers.filter((w) => w.busy).length;
  }

  /**
   * Returns a copy of active worker threads for diagnostics and test verification.
   */
  public getWorkers(): readonly Worker[] {
    return this.workers.map((w) => w.worker);
  }

  public execute(
    jobData: Omit<ProofJobData, "id">,
    timeoutMs = 120_000
  ): Promise<ProveSuccessResponse> {
    if (this.closed) {
      return Promise.reject(new Error("WorkerPool is closed"));
    }

    const id = crypto.randomUUID();
    const job: ProofJobData = { ...jobData, id };

    return new Promise<ProveSuccessResponse>((resolve, reject) => {
      let timer: NodeJS.Timeout | undefined;

      const wrappedResolve = (val: ProveSuccessResponse) => {
        if (timer) clearTimeout(timer);
        resolve(val);
      };

      const wrappedReject = (err: Error) => {
        if (timer) clearTimeout(timer);
        reject(err);
      };

      if (timeoutMs > 0) {
        timer = setTimeout(() => {
          this.taskMap.delete(id);
          wrappedReject(new Error(`Proof generation timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      }

      const pendingTask: PendingTask = {
        job,
        resolve: wrappedResolve,
        reject: wrappedReject,
      };

      this.taskMap.set(id, pendingTask);

      const idleWorker = this.workers.find((w) => !w.busy);
      if (idleWorker) {
        this.dispatch(idleWorker, pendingTask);
      } else {
        this.taskQueue.push(pendingTask);
      }
    });
  }

  private dispatch(workerState: WorkerState, task: PendingTask): void {
    workerState.busy = true;
    workerState.currentTaskId = task.job.id;
    workerState.worker.postMessage(task.job);
  }

  private onWorkerMessage(workerState: WorkerState, msg: ProofJobResult): void {
    workerState.busy = false;
    const taskId = msg.id;
    workerState.currentTaskId = undefined;

    const task = this.taskMap.get(taskId);
    if (task) {
      this.taskMap.delete(taskId);
      if (msg.success && msg.result) {
        task.resolve(msg.result);
      } else {
        task.reject(new Error(msg.error || "Unknown proof generation error"));
      }
    }

    // Process next queued task if available
    const nextTask = this.taskQueue.shift();
    if (nextTask) {
      this.dispatch(workerState, nextTask);
    }
  }

  private onWorkerError(workerState: WorkerState, err: Error): void {
    if (workerState.currentTaskId) {
      const task = this.taskMap.get(workerState.currentTaskId);
      if (task) {
        this.taskMap.delete(workerState.currentTaskId);
        task.reject(err);
      }
    }
    workerState.busy = false;
    workerState.currentTaskId = undefined;
  }

  private restartWorker(
    oldState: WorkerState,
    script: string
  ): void {
    const idx = this.workers.indexOf(oldState);
    if (idx !== -1) {
      const newState = this.createWorkerState(script);
      this.workers[idx] = newState;

      const nextTask = this.taskQueue.shift();
      if (nextTask) {
        this.dispatch(newState, nextTask);
      }
    }
  }

  public async close(): Promise<void> {
    this.closed = true;
    // Drain pending tasks
    for (const task of this.taskMap.values()) {
      task.reject(new Error("WorkerPool closed before task completed"));
    }
    this.taskMap.clear();
    this.taskQueue = [];

    await Promise.all(this.workers.map((w) => w.worker.terminate()));
    this.workers = [];
  }
}
