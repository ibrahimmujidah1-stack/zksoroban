import assert from "node:assert/strict";
import test, { describe } from "node:test";
import { WorkerPool } from "../src/workerPool.js";

describe("WorkerPool Thread Lifecycle", () => {
  test("initializes worker threads and handles file-missing gracefully", async () => {
    const pool = new WorkerPool(2);
    assert.equal(pool.size, 2);
    assert.equal(pool.queueLength, 0);

    // Attempting to execute with non-existent circuit file should reject with descriptive error
    await assert.rejects(
      pool.execute({
        circuit: "poseidon_preimage",
        secret: "123",
        commitment: "456",
        wasmPath: "./non-existent-circuit.wasm",
        zkeyPath: "./non-existent-circuit.zkey",
      }),
      (err: Error) => {
        assert.ok(err.message.includes("not found"));
        return true;
      }
    );

    await pool.close();
  });

  test("restarts worker thread when worker crashes or exits with non-zero code", async () => {
    const pool = new WorkerPool(2);
    assert.equal(pool.size, 2);

    const initialWorkers = pool.getWorkers();
    assert.equal(initialWorkers.length, 2);

    // Simulate unexpected worker crash by terminating the first worker thread
    const terminatedWorker = initialWorkers[0];
    await terminatedWorker.terminate();

    // Give the event loop a brief tick for the 'exit' handler and restartWorker to run
    await new Promise((resolve) => setTimeout(resolve, 60));

    // Pool size should remain 2
    assert.equal(pool.size, 2);

    // The crashed worker should be replaced with a new Worker instance
    const currentWorkers = pool.getWorkers();
    assert.equal(currentWorkers.length, 2);
    assert.notEqual(currentWorkers[0], terminatedWorker);

    // Pool should still process subsequent requests using the restarted worker
    await assert.rejects(
      pool.execute({
        circuit: "poseidon_preimage",
        secret: "123",
        commitment: "456",
        wasmPath: "./non-existent-circuit.wasm",
        zkeyPath: "./non-existent-circuit.zkey",
      }),
      (err: Error) => {
        assert.ok(err.message.includes("not found"));
        return true;
      }
    );

    await pool.close();
  });

  test("aborts in-flight task when worker exits unexpectedly and restarts worker", async () => {
    const pool = new WorkerPool(1);
    assert.equal(pool.size, 1);

    const initialWorker = pool.getWorkers()[0];

    // Submit a task and immediately terminate worker while task is active
    const taskPromise = pool.execute({
      circuit: "poseidon_preimage",
      secret: "123",
      commitment: "456",
      wasmPath: "./non-existent-circuit.wasm",
      zkeyPath: "./non-existent-circuit.zkey",
    });

    // Terminate worker to trigger exit
    await initialWorker.terminate();

    // The in-flight task must be rejected rather than hang
    await assert.rejects(taskPromise, (err: Error) => {
      assert.ok(
        err.message.includes("Worker exited unexpectedly") ||
        err.message.includes("not found")
      );
      return true;
    });

    // Allow restart to complete
    await new Promise((resolve) => setTimeout(resolve, 60));
    assert.equal(pool.size, 1);
    assert.notEqual(pool.getWorkers()[0], initialWorker);

    await pool.close();
  });
});
