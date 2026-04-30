import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";
import { indexedDB as fakeIndexedDB } from "fake-indexeddb";
import { withFileLock } from "openclaw/plugin-sdk/file-lock";
import { MATRIX_IDB_SNAPSHOT_LOCK_OPTIONS } from "./idb-persistence-lock.js";
import { LogService } from "./logger.js";

// Advisory lock options for IDB snapshot file access. Without locking, the
// gateway's periodic 60-second persist cycle and CLI crypto commands (e.g.
// `openclaw matrix verify bootstrap`) can corrupt each other's state.
// Use a longer stale window than the generic 30s default because snapshot
// restore and large crypto-store dumps can legitimately hold the lock for
// longer, and reclaiming a live lock would reintroduce concurrent corruption.
type IdbStoreSnapshot = {
  name: string;
  keyPath: IDBObjectStoreParameters["keyPath"];
  autoIncrement: boolean;
  indexes: { name: string; keyPath: string | string[]; multiEntry: boolean; unique: boolean }[];
  records: { key: IDBValidKey; value: unknown }[];
};

type IdbDatabaseSnapshot = {
  name: string;
  version: number;
  stores: IdbStoreSnapshot[];
};

function isValidIdbIndexSnapshot(value: unknown): value is IdbStoreSnapshot["indexes"][number] {
  if (!value || typeof value !== "object") {
    return false;
  }
  const candidate = value as Partial<IdbStoreSnapshot["indexes"][number]>;
  return (
    typeof candidate.name === "string" &&
    (typeof candidate.keyPath === "string" ||
      (Array.isArray(candidate.keyPath) &&
        candidate.keyPath.every((entry) => typeof entry === "string"))) &&
    typeof candidate.multiEntry === "boolean" &&
    typeof candidate.unique === "boolean"
  );
}

function isValidIdbRecordSnapshot(value: unknown): value is IdbStoreSnapshot["records"][number] {
  if (!value || typeof value !== "object") {
    return false;
  }
  return "key" in value && "value" in value;
}

function isValidIdbStoreSnapshot(value: unknown): value is IdbStoreSnapshot {
  if (!value || typeof value !== "object") {
    return false;
  }
  const candidate = value as Partial<IdbStoreSnapshot>;
  const validKeyPath =
    candidate.keyPath === null ||
    typeof candidate.keyPath === "string" ||
    (Array.isArray(candidate.keyPath) &&
      candidate.keyPath.every((entry) => typeof entry === "string"));
  return (
    typeof candidate.name === "string" &&
    validKeyPath &&
    typeof candidate.autoIncrement === "boolean" &&
    Array.isArray(candidate.indexes) &&
    candidate.indexes.every((entry) => isValidIdbIndexSnapshot(entry)) &&
    Array.isArray(candidate.records) &&
    candidate.records.every((entry) => isValidIdbRecordSnapshot(entry))
  );
}

function isValidIdbDatabaseSnapshot(value: unknown): value is IdbDatabaseSnapshot {
  if (!value || typeof value !== "object") {
    return false;
  }
  const candidate = value as Partial<IdbDatabaseSnapshot>;
  return (
    typeof candidate.name === "string" &&
    typeof candidate.version === "number" &&
    Number.isFinite(candidate.version) &&
    candidate.version > 0 &&
    Array.isArray(candidate.stores) &&
    candidate.stores.every((entry) => isValidIdbStoreSnapshot(entry))
  );
}

function parseSnapshotPayload(data: string): IdbDatabaseSnapshot[] | null {
  const parsed = JSON.parse(data) as unknown;
  if (!Array.isArray(parsed) || parsed.length === 0) {
    return null;
  }
  if (!parsed.every((entry) => isValidIdbDatabaseSnapshot(entry))) {
    throw new Error("Malformed IndexedDB snapshot payload");
  }
  return parsed;
}

function idbReq<T>(req: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    req.addEventListener("success", () => resolve(req.result), { once: true });
    req.addEventListener("error", () => reject(req.error), { once: true });
  });
}

async function dumpIndexedDatabases(databasePrefix?: string): Promise<IdbDatabaseSnapshot[]> {
  const idb = fakeIndexedDB;
  const dbList = await idb.databases();
  const snapshot: IdbDatabaseSnapshot[] = [];
  const expectedPrefix = databasePrefix ? `${databasePrefix}::` : null;

  for (const { name, version } of dbList) {
    if (!name || !version) {
      continue;
    }
    if (expectedPrefix && !name.startsWith(expectedPrefix)) {
      continue;
    }
    const db: IDBDatabase = await new Promise((resolve, reject) => {
      const r = idb.open(name, version);
      r.addEventListener("success", () => resolve(r.result), { once: true });
      r.addEventListener("error", () => reject(r.error), { once: true });
    });

    const stores: IdbStoreSnapshot[] = [];
    for (const storeName of db.objectStoreNames) {
      const tx = db.transaction(storeName, "readonly");
      const store = tx.objectStore(storeName);
      const storeInfo: IdbStoreSnapshot = {
        name: storeName,
        keyPath: store.keyPath as IDBObjectStoreParameters["keyPath"],
        autoIncrement: store.autoIncrement,
        indexes: [],
        records: [],
      };
      for (const idxName of store.indexNames) {
        const idx = store.index(idxName);
        storeInfo.indexes.push({
          name: idxName,
          keyPath: idx.keyPath,
          multiEntry: idx.multiEntry,
          unique: idx.unique,
        });
      }
      const keys = await idbReq(store.getAllKeys());
      const values = await idbReq(store.getAll());
      storeInfo.records = keys.map((k, i) => ({ key: k, value: values[i] }));
      stores.push(storeInfo);
    }
    snapshot.push({ name, version, stores });
    db.close();
  }
  return snapshot;
}

async function restoreIndexedDatabases(snapshot: IdbDatabaseSnapshot[]): Promise<void> {
  const idb = fakeIndexedDB;
  for (const dbSnap of snapshot) {
    await new Promise<void>((resolve, reject) => {
      const r = idb.open(dbSnap.name, dbSnap.version);
      r.addEventListener("upgradeneeded", () => {
        const db = r.result;
        for (const storeSnap of dbSnap.stores) {
          const opts: IDBObjectStoreParameters = {};
          if (storeSnap.keyPath !== null) {
            opts.keyPath = storeSnap.keyPath;
          }
          if (storeSnap.autoIncrement) {
            opts.autoIncrement = true;
          }
          const store = db.createObjectStore(storeSnap.name, opts);
          for (const idx of storeSnap.indexes) {
            store.createIndex(idx.name, idx.keyPath, {
              unique: idx.unique,
              multiEntry: idx.multiEntry,
            });
          }
        }
      });
      r.addEventListener(
        "success",
        () => {
          void (async () => {
            const db = r.result;
            for (const storeSnap of dbSnap.stores) {
              if (storeSnap.records.length === 0) {
                continue;
              }
              const tx = db.transaction(storeSnap.name, "readwrite");
              const store = tx.objectStore(storeSnap.name);
              for (const rec of storeSnap.records) {
                if (storeSnap.keyPath !== null) {
                  store.put(rec.value);
                } else {
                  store.put(rec.value, rec.key);
                }
              }
              await new Promise<void>((res) => {
                tx.addEventListener("complete", () => res(), { once: true });
              });
            }
            db.close();
            resolve();
          })().catch(reject);
        },
        { once: true },
      );
      r.addEventListener("error", () => reject(r.error), { once: true });
    });
  }
}

function resolveDefaultIdbSnapshotPath(): string {
  const stateDir =
    process.env.OPENCLAW_STATE_DIR || path.join(process.env.HOME || "/tmp", ".openclaw");
  return path.join(stateDir, "matrix", "crypto-idb-snapshot.json");
}

export async function restoreIdbFromDisk(snapshotPath?: string): Promise<boolean> {
  const candidatePaths = snapshotPath ? [snapshotPath] : [resolveDefaultIdbSnapshotPath()];
  for (const resolvedPath of candidatePaths) {
    if (!fs.existsSync(resolvedPath)) {
      continue;
    }
    try {
      const restored = await withFileLock(
        resolvedPath,
        MATRIX_IDB_SNAPSHOT_LOCK_OPTIONS,
        async () => {
          const data = fs.readFileSync(resolvedPath, "utf8");
          const snapshot = parseSnapshotPayload(data);
          if (!snapshot) {
            return false;
          }
          await restoreIndexedDatabases(snapshot);
          LogService.info(
            "IdbPersistence",
            `Restored ${snapshot.length} IndexedDB database(s) from ${resolvedPath}`,
          );
          return true;
        },
      );
      if (restored) {
        return true;
      }
    } catch (err) {
      LogService.warn(
        "IdbPersistence",
        `Failed to restore IndexedDB snapshot from ${resolvedPath}:`,
        err,
      );
      continue;
    }
  }
  return false;
}

export async function persistIdbToDisk(params?: {
  snapshotPath?: string;
  databasePrefix?: string;
}): Promise<void> {
  const snapshotPath = params?.snapshotPath ?? resolveDefaultIdbSnapshotPath();
  try {
    fs.mkdirSync(path.dirname(snapshotPath), { recursive: true });
    const persistedCount = await withFileLock(
      snapshotPath,
      MATRIX_IDB_SNAPSHOT_LOCK_OPTIONS,
      async () => {
        const snapshot = await dumpIndexedDatabases(params?.databasePrefix);
        if (snapshot.length === 0) {
          return 0;
        }
        await writeSnapshotToDisk(snapshot, snapshotPath);
        return snapshot.length;
      },
    );
    if (persistedCount === 0) {
      return;
    }
    LogService.debug(
      "IdbPersistence",
      `Persisted ${persistedCount} IndexedDB database(s) to ${snapshotPath}`,
    );
  } catch (err) {
    LogService.warn("IdbPersistence", "Failed to persist IndexedDB snapshot:", err);
  }
}

// ---------------------------------------------------------------------------
// Off-thread serialization
// ---------------------------------------------------------------------------
//
// The matrix-sdk-crypto-wasm runtime stores its crypto state in
// fake-indexeddb. JSON.stringify of the full snapshot plus the
// fs.writeFileSync that follows ran on the main event loop and blocked
// it for tens of seconds at a time as the crypto store grew (production
// observed 47s and 155s windows, see argocd-apps deployment.yaml notes).
//
// We dispatch the serialize+write step to a long-lived worker_thread so
// the main thread is unblocked between dumpIndexedDatabases (which is
// async-IDB and yields naturally) and the next persist tick. The
// withFileLock wrapper stays on the main thread; it holds the advisory
// lock for the duration of the dispatch round-trip, preserving the
// concurrency guarantees the previous main-thread implementation
// already relied on.
//
// Kill-switch: setting MATRIX_IDB_PERSIST_WORKER=off forces the legacy
// inline path (still atomic-write — that fix applies to both branches).
//
// Override: MATRIX_IDB_PERSIST_WORKER_PATH lets tests / debugging
// substitute an alternate worker entry point. Default resolves to the
// sibling `idb-persistence-worker.js` next to this file in the built
// dist tree.

type PersistMessageRequest = {
  type: "persist";
  id: number;
  snapshot: IdbDatabaseSnapshot[];
  snapshotPath: string;
};

type PersistMessageResponse = {
  type: "persistResult";
  id: number;
  ok: boolean;
  error?: string;
};

type PendingPersist = {
  resolve: () => void;
  reject: (err: Error) => void;
};

const persistWorkerEnabled = (): boolean => {
  return (process.env.MATRIX_IDB_PERSIST_WORKER ?? "").toLowerCase() !== "off";
};

let persistWorkerHandle: Worker | null = null;
let persistWorkerSpawning: Promise<Worker> | null = null;
const pendingPersists = new Map<number, PendingPersist>();
let nextPersistId = 1;

function resolvePersistWorkerEntry(): string {
  const override = process.env.MATRIX_IDB_PERSIST_WORKER_PATH?.trim();
  if (override) {
    return override;
  }
  // Default: sibling .js artifact emitted by tsdown next to this file.
  // Both source and built tree place idb-persistence-worker as a sibling
  // of idb-persistence; resolving relative to import.meta.url works in
  // either layout since fileURLToPath ignores the .js/.ts mismatch when
  // the file actually exists.
  const sibling = new URL("./idb-persistence-worker.js", import.meta.url);
  return fileURLToPath(sibling);
}

async function spawnPersistWorker(): Promise<Worker> {
  if (persistWorkerHandle) {
    return persistWorkerHandle;
  }
  if (persistWorkerSpawning) {
    return await persistWorkerSpawning;
  }
  persistWorkerSpawning = (async () => {
    const entry = resolvePersistWorkerEntry();
    if (!fs.existsSync(entry)) {
      throw new Error(
        `Matrix IDB persist worker entry not found: ${entry}. Build the matrix extension or set MATRIX_IDB_PERSIST_WORKER_PATH, or set MATRIX_IDB_PERSIST_WORKER=off to fall back to the inline path.`,
      );
    }
    const worker = new Worker(entry, {
      name: "matrix-idb-persist",
    });
    worker.unref();
    worker.on("message", (msg: PersistMessageResponse) => {
      if (!msg || msg.type !== "persistResult") {
        return;
      }
      const pending = pendingPersists.get(msg.id);
      if (!pending) {
        return;
      }
      pendingPersists.delete(msg.id);
      if (msg.ok) {
        pending.resolve();
      } else {
        pending.reject(new Error(msg.error ?? "persist worker reported failure"));
      }
    });
    worker.on("error", (err) => {
      LogService.warn("IdbPersistence", "Persist worker error:", err);
    });
    worker.on("exit", (code) => {
      const exitedHandle = persistWorkerHandle;
      persistWorkerHandle = null;
      persistWorkerSpawning = null;
      const exitErr = new Error(`Persist worker exited (code=${code})`);
      for (const pending of pendingPersists.values()) {
        pending.reject(exitErr);
      }
      pendingPersists.clear();
      if (code !== 0 && exitedHandle) {
        LogService.warn("IdbPersistence", `Persist worker exited unexpectedly with code ${code}`);
      }
    });
    persistWorkerHandle = worker;
    return worker;
  })();
  try {
    return await persistWorkerSpawning;
  } finally {
    persistWorkerSpawning = null;
  }
}

async function persistViaWorker(
  snapshot: IdbDatabaseSnapshot[],
  snapshotPath: string,
): Promise<void> {
  const worker = await spawnPersistWorker();
  const id = nextPersistId++;
  return await new Promise<void>((resolve, reject) => {
    pendingPersists.set(id, { resolve, reject });
    const request: PersistMessageRequest = {
      type: "persist",
      id,
      snapshot,
      snapshotPath,
    };
    try {
      worker.postMessage(request);
    } catch (err) {
      pendingPersists.delete(id);
      reject(
        err instanceof Error
          ? err
          : new Error(`postMessage to persist worker failed: ${String(err)}`),
      );
    }
  });
}

async function writeSnapshotToDisk(
  snapshot: IdbDatabaseSnapshot[],
  snapshotPath: string,
): Promise<void> {
  if (persistWorkerEnabled()) {
    try {
      await persistViaWorker(snapshot, snapshotPath);
      return;
    } catch (err) {
      LogService.warn(
        "IdbPersistence",
        "Persist worker dispatch failed; falling back to inline write:",
        err,
      );
      // fall through
    }
  }
  writeSnapshotInline(snapshot, snapshotPath);
}

function writeSnapshotInline(snapshot: IdbDatabaseSnapshot[], snapshotPath: string): void {
  // Atomic write applies to the inline path too: produce the same
  // tmp → fsync → rename → mode 0o600 result the worker would.
  const body = JSON.stringify(snapshot);
  const tmpPath = `${snapshotPath}.tmp.${process.pid}`;
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
        // ignore
      }
    }
    try {
      fs.unlinkSync(tmpPath);
    } catch {
      // ignore — tmp may not exist if openSync failed
    }
    throw err;
  }
  fs.closeSync(fd);
  fs.renameSync(tmpPath, snapshotPath);
}

/**
 * Stop the persist worker. Resolves once any in-flight persist completes
 * (or rejects via the worker's exit handler). Safe to call when no
 * worker has been spawned.
 */
export async function terminatePersistWorker(): Promise<void> {
  const worker = persistWorkerHandle;
  if (!worker) {
    return;
  }
  // Wait for any in-flight persists so a final shutdown persist that
  // raced ahead of terminate doesn't get cancelled mid-write.
  if (pendingPersists.size > 0) {
    const inFlight = Array.from(pendingPersists.values());
    await Promise.allSettled(
      inFlight.map(
        (entry) =>
          new Promise<void>((resolve) => {
            const wrap = entry.resolve;
            const wrapErr = entry.reject;
            entry.resolve = () => {
              try {
                wrap();
              } finally {
                resolve();
              }
            };
            entry.reject = (err) => {
              try {
                wrapErr(err);
              } finally {
                resolve();
              }
            };
          }),
      ),
    );
  }
  try {
    worker.postMessage({ type: "shutdown" });
  } catch {
    // Worker may have already exited; falls through to terminate().
  }
  await worker.terminate();
}
