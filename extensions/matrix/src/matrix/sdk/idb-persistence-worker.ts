// Worker entrypoint for off-thread IndexedDB snapshot serialization.
//
// The matrix-sdk-crypto-wasm runtime stores its crypto state in
// fake-indexeddb. Persisting that store to disk every 60s requires
// JSON.stringify of the entire snapshot followed by fs.writeFileSync —
// both synchronous, both blocking the main event loop for the duration.
// Production has observed 47s and 155s blocks during this window
// (argocd-apps/epyc/openclaw/deployment.yaml documents this).
//
// This worker receives a structuredClone of the dumped snapshot from
// the main thread, runs JSON.stringify + an atomic write here, and
// replies. The main thread keeps holding the file lock across the
// dispatch + reply window so concurrent CLI invocations still see a
// well-defined state.
//
// Atomic write: open a sibling .tmp.<pid> with mode 0o600 → write →
// fsync → close → rename. rename(2) is atomic within a filesystem on
// POSIX, so a process crash mid-write either leaves the prior valid
// snapshot in place (if the rename never happened) or the fully-fsync'd
// new one (if it did). The previous main-thread implementation was
// non-atomic and could leave a half-written JSON on OOM-kill, which
// silently failed restore on the next pod boot and triggered a fresh
// device upload.

import fs from "node:fs";
import { parentPort } from "node:worker_threads";

export type PersistRequest = {
  type: "persist";
  id: number;
  snapshot: unknown;
  snapshotPath: string;
};

export type ShutdownRequest = {
  type: "shutdown";
};

export type WorkerRequest = PersistRequest | ShutdownRequest;

export type PersistResponse = {
  type: "persistResult";
  id: number;
  ok: boolean;
  error?: string;
};

if (!parentPort) {
  // The file is invoked via `new Worker(...)`; if parentPort is null we
  // were loaded as a regular module and there's nothing for us to do.
  // Bail rather than crash so accidental imports don't break a host.
} else {
  const port = parentPort;
  port.on("message", (msg: WorkerRequest) => {
    if (!msg || typeof msg !== "object") {
      return;
    }
    if (msg.type === "shutdown") {
      port.close();
      return;
    }
    if (msg.type === "persist") {
      handlePersist(msg, (response) => port.postMessage(response));
    }
  });
}

export function handlePersist(
  req: PersistRequest,
  reply: (response: PersistResponse) => void,
): void {
  let body: string;
  try {
    body = JSON.stringify(req.snapshot);
  } catch (err) {
    reply({
      type: "persistResult",
      id: req.id,
      ok: false,
      error: `JSON.stringify failed: ${formatError(err)}`,
    });
    return;
  }
  if (typeof body !== "string") {
    reply({
      type: "persistResult",
      id: req.id,
      ok: false,
      error: "JSON.stringify returned undefined (snapshot is not serialisable)",
    });
    return;
  }

  const tmpPath = `${req.snapshotPath}.tmp.${process.pid}`;
  let fd: number | null = null;
  try {
    fd = fs.openSync(tmpPath, "w", 0o600);
    fs.writeFileSync(fd, body);
    fs.fsyncSync(fd);
  } catch (err) {
    if (fd !== null) {
      try {
        fs.closeSync(fd);
      } catch {
        // already closed or never opened cleanly; nothing useful to do
      }
    }
    safeUnlink(tmpPath);
    reply({
      type: "persistResult",
      id: req.id,
      ok: false,
      error: `Failed writing tmp snapshot: ${formatError(err)}`,
    });
    return;
  }
  try {
    fs.closeSync(fd);
  } catch (err) {
    safeUnlink(tmpPath);
    reply({
      type: "persistResult",
      id: req.id,
      ok: false,
      error: `Failed closing tmp snapshot fd: ${formatError(err)}`,
    });
    return;
  }
  try {
    fs.renameSync(tmpPath, req.snapshotPath);
  } catch (err) {
    safeUnlink(tmpPath);
    reply({
      type: "persistResult",
      id: req.id,
      ok: false,
      error: `Failed atomic rename ${tmpPath} -> ${req.snapshotPath}: ${formatError(err)}`,
    });
    return;
  }
  reply({ type: "persistResult", id: req.id, ok: true });
}

function safeUnlink(filePath: string): void {
  try {
    fs.unlinkSync(filePath);
  } catch {
    // tmp may not exist (open failed before creating it) — ignore.
  }
}

function formatError(err: unknown): string {
  if (err instanceof Error) {
    return `${err.name}: ${err.message}`;
  }
  return String(err);
}
