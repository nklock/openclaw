import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  handlePersist,
  type PersistRequest,
  type PersistResponse,
} from "./idb-persistence-worker.js";

describe("idb-persistence-worker handlePersist", () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "matrix-idb-persist-worker-"));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  function captureReply(): {
    promise: Promise<PersistResponse>;
    reply: (response: PersistResponse) => void;
  } {
    let resolve: (value: PersistResponse) => void = () => {};
    const promise = new Promise<PersistResponse>((res) => {
      resolve = res;
    });
    return {
      promise,
      reply: (response) => resolve(response),
    };
  }

  it("writes snapshot atomically (tmp -> rename) with mode 0o600", async () => {
    const snapshotPath = path.join(tmpDir, "snap.json");
    const snapshot = [{ name: "db", version: 1, stores: [{ name: "s", records: [] }] }];
    const { promise, reply } = captureReply();
    const req: PersistRequest = {
      type: "persist",
      id: 1,
      snapshot,
      snapshotPath,
    };

    handlePersist(req, reply);
    const result = await promise;

    expect(result).toEqual({ type: "persistResult", id: 1, ok: true });
    expect(fs.existsSync(snapshotPath)).toBe(true);
    const mode = fs.statSync(snapshotPath).mode & 0o777;
    expect(mode).toBe(0o600);
    expect(JSON.parse(fs.readFileSync(snapshotPath, "utf8"))).toEqual(snapshot);

    // No leftover tmp file.
    const dirEntries = fs.readdirSync(tmpDir);
    expect(dirEntries).toEqual(["snap.json"]);
  });

  it("preserves prior snapshot if a write fails partway", () => {
    // Pre-seed a valid snapshot, then attempt a write that targets a
    // path the worker cannot create (parent dir missing). The atomic
    // write should leave the prior snapshot intact.
    const snapshotPath = path.join(tmpDir, "snap.json");
    fs.writeFileSync(snapshotPath, JSON.stringify([{ existing: true }]), { mode: 0o600 });

    const badPath = path.join(tmpDir, "missing-subdir", "snap.json");
    const { reply } = captureReply();
    const responses: PersistResponse[] = [];
    handlePersist(
      { type: "persist", id: 2, snapshot: [{ new: true }], snapshotPath: badPath },
      (r) => {
        responses.push(r);
        reply(r);
      },
    );

    expect(responses[0]?.ok).toBe(false);
    expect(responses[0]?.error).toMatch(/Failed writing tmp snapshot/);
    // Original file untouched.
    expect(JSON.parse(fs.readFileSync(snapshotPath, "utf8"))).toEqual([{ existing: true }]);
  });

  it("reports JSON.stringify failures cleanly without writing a partial file", async () => {
    const snapshotPath = path.join(tmpDir, "snap.json");
    // Create a circular reference — JSON.stringify will throw.
    const circular: Record<string, unknown> = { name: "cycle" };
    circular.self = circular;
    const { promise, reply } = captureReply();

    handlePersist(
      { type: "persist", id: 3, snapshot: circular, snapshotPath },
      reply,
    );
    const result = await promise;

    expect(result.ok).toBe(false);
    expect(result.error).toMatch(/JSON\.stringify failed/);
    expect(fs.existsSync(snapshotPath)).toBe(false);
    // No tmp leftover.
    const dirEntries = fs.readdirSync(tmpDir);
    expect(dirEntries).toEqual([]);
  });

  it("overwrites an existing snapshot via atomic rename", async () => {
    const snapshotPath = path.join(tmpDir, "snap.json");
    fs.writeFileSync(snapshotPath, JSON.stringify([{ old: 1 }]), { mode: 0o600 });

    const { promise, reply } = captureReply();
    handlePersist(
      {
        type: "persist",
        id: 4,
        snapshot: [{ fresh: 2 }],
        snapshotPath,
      },
      reply,
    );
    const result = await promise;

    expect(result.ok).toBe(true);
    expect(JSON.parse(fs.readFileSync(snapshotPath, "utf8"))).toEqual([{ fresh: 2 }]);
    const mode = fs.statSync(snapshotPath).mode & 0o777;
    expect(mode).toBe(0o600);
  });
});
