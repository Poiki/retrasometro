import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { DB } from "../src/db";
import { config } from "../src/config";
import { RenfeIngestor } from "../src/ingestor";
import { DashboardPresetCache } from "../src/dashboard-cache";
import { OpsPresetCache } from "../src/ops-cache";
import { startServer } from "../src/server";
import type { RunAnalytics } from "../src/analytics-worker";
import type { NormalizedTrain } from "../src/types";

const dir = mkdtempSync(join(tmpdir(), "retrasometro-check-"));
const originalConfig = { ...config };
const originalFetch = globalThis.fetch;
const db = new DB(join(dir, "test.db"));
const sql = new Database(join(dir, "test.db"));
const train: NormalizedTrain = {
  codComercial: "12345", codProduct: 2, codOrigen: "O", codDestino: "D",
  codEstAnt: null, codEstSig: "D", horaLlegadaSigEst: null, desCorridor: "Ruta",
  accesible: 1, ultRetraso: 16, latitud: 40.123456789, longitud: -3.123456789,
  gpsTime: 1773252000, p: null, mat: "material",
};
const now = Math.floor(Date.now() / 1000);
const old = now - 100 * 86400;
try {
  db.insertSnapshot(train, old, "hash");
  db.insertTrainObservations(null, old, [{ train, hash: "hash" }], "live", 0);
  assert.equal(db.recoverObservationsFromSnapshots(old), 0, "recovery must not duplicate live observations");
  db.insertSnapshot({ ...train, codComercial: "54321" }, old, "missing");
  assert.equal(db.recoverObservationsFromSnapshots(old), 1, "missing observations remain recoverable");
  assert.equal(db.recoverObservationsFromSnapshots(old), 0, "recovery is idempotent");

  Object.assign(config, {
    snapshotRetentionHours: 0, ingestionRunRetentionDays: 0, observationRetentionDays: 0,
    hourlyRetentionDays: 0, dailyRetentionDays: 0, batchRetentionDays: 0,
    compactEveryRuns: 1, cacheFile: join(dir, "cache.json"), archiveEnabled: false,
  });
  db.recordIngestionRun({ fetchedAt: old, source: "test", success: 1, trainCount: 1, skipped: 0, error: null, providerUpdatedAt: null });
  const ingestor = new RenfeIngestor(db);
  Object.defineProperty(ingestor, "runCounter", { value: 1, writable: true });
  globalThis.fetch = Object.assign(async () => Response.json({ fechaActualizacion: "test", trenes: [{ codComercial: "99999", codProduct: 2, latitud: 40, longitud: -3 }] }), { preconnect: originalFetch.preconnect });
  await ingestor.runOnce();
  assert.equal((sql.query("SELECT COUNT(*) AS n FROM train_snapshots WHERE captured_at=?").get(old) as { n: number }).n, 2);
  assert.equal((sql.query("SELECT COUNT(*) AS n FROM ingestion_runs WHERE fetched_at=?").get(old) as { n: number }).n, 1);

  sql.exec("CREATE TRIGGER fail_hourly BEFORE INSERT ON train_hourly_train_stats BEGIN SELECT RAISE(ABORT,'test failure'); END");
  const before = sql.query("SELECT COUNT(*) AS n FROM ingestion_batches").get() as { n: number };
  globalThis.fetch = Object.assign(async () => Response.json({ fechaActualizacion: "failed", trenes: [{ codComercial: "88888", codProduct: 2, latitud: 40, longitud: -3 }] }), { preconnect: originalFetch.preconnect });
  await ingestor.runOnce();
  assert.equal((sql.query("SELECT COUNT(*) AS n FROM ingestion_batches").get() as { n: number }).n, before.n);
  assert.equal(sql.query("SELECT 1 FROM trains_current WHERE cod_comercial='88888'").get(), null);
  sql.exec("DROP TRIGGER fail_hourly");

  const rowsBefore = sql.query("SELECT * FROM train_observations WHERE captured_at=? ORDER BY id").all(old);
  const result = db.archiveObservationsBefore(now - 86400, join(dir, "archive"), 3);
  assert.equal(result.archivedRows, 2);
  const manifest = sql.query("SELECT * FROM observation_archive_chunks").get() as { file_path: string };
  const decoded = Bun.spawnSync(["zstd", "-q", "-d", "-c", manifest.file_path]);
  assert.equal(decoded.exitCode, 0);
  const archived = Buffer.from(decoded.stdout).toString().trim().split("\n").map((line) => JSON.parse(line));
  assert.deepEqual(archived, rowsBefore, "archive must preserve all columns, ids and coordinates");
  const history = db.getTrainHistory(train.codComercial, 101 * 24);
  assert.equal(history.observations.length, 1, "archived detail must remain queryable");
  const archivedStats = db.getHistoricalStatsCustom(old, old + 60);
  assert.equal(archivedStats.summary.observations, 2);
  assert.equal((archivedStats.byProduct[0] as { observations: number }).observations, 2);
  const hourlyStats = db.getHistoricalStatsCustom(old, old + 60, { preferAggregated: true });
  assert.deepEqual({ ...hourlyStats, summary: { ...hourlyStats.summary, minTs: archivedStats.summary.minTs, maxTs: archivedStats.summary.maxTs } }, archivedStats, "hourly boundaries must preserve archived totals and rankings");

  db.insertTrainObservations(null, old, [{ train: { ...train, codComercial: "11111" }, hash: "new" }], "live", 0);
  db.archiveObservationsBefore(now - 86400, join(dir, "archive"), 3);
  assert.equal((sql.query("SELECT COUNT(*) AS n FROM train_observations WHERE captured_at=?").get(old) as { n: number }).n, 1, "existing manifest must never delete unverified rows");
  const file = readFileSync(manifest.file_path);
  writeFileSync(manifest.file_path, "corrupt");
  assert.throws(() => db.archiveObservationsBefore(now - 86400, join(dir, "archive"), 3));
  assert.equal((sql.query("SELECT COUNT(*) AS n FROM train_observations WHERE captured_at=?").get(old) as { n: number }).n, 1);
  writeFileSync(manifest.file_path, file);
  globalThis.fetch = originalFetch;
  config.displayCacheDir = join(dir, "display");
  config.port = 0;
  const dashboard = new DashboardPresetCache(db);
  const ops = new OpsPresetCache(db);
  await dashboard.generate(168);
  await ops.generate(168);
  const analytics: RunAnalytics = async (method, ...args) => {
    await Bun.sleep(300);
    if (method === "getHistoricalStatsCustom") return db.getHistoricalStatsCustom(...args as Parameters<DB["getHistoricalStatsCustom"]>) as never;
    throw new Error("unexpected analytics request");
  };
  const meta = db.getCorridorMeta;
  const metrics = db.getOpsMetrics;
  db.getCorridorMeta = () => { throw new Error("cached route must not scan observations"); };
  db.getOpsMetrics = () => { throw new Error("cached route must not compute Ops"); };
  const events = new EventTarget();
  const server = startServer(db, ingestor, dashboard, ops, analytics, events);
  try {
    const base = `http://localhost:${server.port}`;
    const key = async () => (await (await fetch(`${base}/api/auth/request-key`)).json() as { apiKey: string }).apiKey;
    assert.equal((await fetch(`${base}/api/dashboard`)).status, 401);
    const dashboardResponse = await fetch(`${base}/api/dashboard?historyHours=168&limit=1&q=99999`, { headers: { "x-api-key": await key() } });
    assert.equal(dashboardResponse.status, 200);
    const payload = await dashboardResponse.json() as { trains: { total: number; items: Array<{ cod_comercial: string }> } };
    assert.equal(payload.trains.total, 1);
    assert.equal(payload.trains.items[0]?.cod_comercial, "99999");
    assert.equal((await fetch(`${base}/api/ops`, { headers: { "x-api-key": await key() } })).status, 200);
    const sharedKey = await key();
    const slow = fetch(`${base}/api/dashboard?historyFrom=${now-60}&historyTo=${now}`, { headers: { "x-api-key": sharedKey } });
    await Bun.sleep(30);
    const concurrent = await fetch(`${base}/api/dashboard`, { headers: { "x-api-key": sharedKey } });
    assert.equal(concurrent.status, 429);
    assert.equal((await concurrent.json() as { code: string }).code, "IN_FLIGHT_REQUEST");
    const begin = performance.now();
    assert.equal((await fetch(`${base}/api/health`)).status, 200);
    assert.ok(performance.now() - begin < 200, "history must not block health");
    assert.equal((await slow).status, 200);
    const fastKey = await key();
    assert.equal((await fetch(`${base}/api/dashboard`, { headers: { "x-api-key": fastKey } })).status, 200);
    const limited = await fetch(`${base}/api/dashboard`, { headers: { "x-api-key": fastKey } });
    assert.equal(limited.status, 429);
    assert.equal((await limited.json() as { code: string }).code, "RATE_LIMITED");
    const cachedEntry = (await dashboard.read(168))!;
    rmSync(join(config.displayCacheDir, "dashboard.168h.json"));
    let generated: Promise<unknown> | null = null;
    dashboard.generate = () => {
      const job = Bun.sleep(300).then(() => cachedEntry);
      generated = job;
      return job;
    };
    const coldStart = performance.now();
    const cold = await fetch(`${base}/api/dashboard`, { headers: { "x-api-key": await key() } });
    const coldPayload = await cold.json() as { historicalPending: boolean; trains: { total: number } };
    assert.equal(coldPayload.historicalPending, true);
    assert.equal(coldPayload.trains.total, 1);
    assert.ok(performance.now() - coldStart < 200, "cold historical cache must not delay live data");
    await generated;
    const abort = new AbortController();
    const stream = await fetch(`${base}/api/events`, { signal: abort.signal, headers: { "accept-encoding": "gzip" } });
    assert.equal(stream.headers.get("content-type"), "text/event-stream");
    assert.equal(stream.headers.get("content-encoding"), null);
    const reader = stream.body!.getReader();
    assert.match(new TextDecoder().decode((await reader.read()).value), /kind.*update/);
    await Bun.sleep(11000);
    events.dispatchEvent(new Event("history"));
    assert.match(new TextDecoder().decode((await reader.read()).value), /kind.*history/);
    abort.abort();
  } finally {
    server.stop(true);
    db.getCorridorMeta = meta;
    db.getOpsMetrics = metrics;
  }
  const worker = new Worker(new URL("../src/analytics-worker.ts", import.meta.url).href, { env: {
    ...process.env, DB_PATH: join(dir, "test.db"), DISPLAY_CACHE_DIR: join(dir, "worker-display"),
  } as Record<string, string> });
  try {
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("analytics worker timeout")), 10000);
      worker.onerror = (error) => { clearTimeout(timeout); reject(new Error(error.message)); };
      worker.onmessage = (event) => {
        if (event.data.id !== 1) return;
        clearTimeout(timeout);
        try {
          assert.equal(event.data.error, undefined);
          assert.equal(event.data.result.dbRows, 2, "worker must use the supplied database, never the live one");
          resolve();
        } catch (error) { reject(error); }
      };
      worker.postMessage({ id: 1, method: "getObservationArchiveStorage", args: [] });
    });
  } finally { worker.terminate(); }
  console.log("OK: recovery, retention, atomic ingestion, archives, hourly boundaries, cache, auth, concurrent API and SSE");
} finally {
  Object.assign(config, originalConfig);
  globalThis.fetch = originalFetch;
  sql.close();
  db.close();
  rmSync(dir, { recursive: true, force: true });
}
