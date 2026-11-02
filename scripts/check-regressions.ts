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
import { calculateMapAnalytics, calculateMapDelayZones, type MapObservation } from "../src/map-analytics";
import { clipRailwayLine, getRailwaySegments } from "../src/railway-geometry";

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
  const square = [[0, 0], [1, 1]];
  assert.deepEqual(clipRailwayLine([[-1, 0.5], [2, 0.5]], square), [[[0, 0.5], [1, 0.5]]]);
  assert.deepEqual(clipRailwayLine([[2, 0.5], [-1, 0.5]], square), [[[1, 0.5], [0, 0.5]]]);
  assert.deepEqual(clipRailwayLine([[-1, 0.2], [-1, 0.8]], square), []);
  assert.deepEqual(clipRailwayLine([[-1, -1], [0, 0]], square), [], "point contacts must not become black dots");
  assert.deepEqual(clipRailwayLine([[0, 0], [1, 0]], square), [[[0, 0], [1, 0]]]);
  const curvedRail: [number, number][] = [[0.2, 0.2], [0.4, 0.8], [0.9, 0.3]];
  assert.deepEqual(clipRailwayLine(curvedRail, square), [curvedRail], "real track curvature must be retained");
  assert.deepEqual(clipRailwayLine([[0.5, 0.5], [2, 0.5], [2, 0.8], [0.5, 0.8]], square), [
    [[0.5, 0.5], [1, 0.5]], [[1, 0.8], [0.5, 0.8]],
  ], "disjoint rail portions must never be joined across an unflagged area");
  const actualRails = getRailwaySegments([[40.4, -3.7], [40.425, -3.675]]);
  assert.ok(actualRails.length > 0, "the snapshot includes the real Atocha railway geometry");
  assert.ok(actualRails.every((line) => line.length >= 2 && line.every(([lat, lon]) => lat >= 40.4 - 1e-9 && lat <= 40.425 + 1e-9 && lon >= -3.7 - 1e-9 && lon <= -3.675 + 1e-9)));
  assert.deepEqual(getRailwaySegments([[-70, -100], [-69.975, -99.975]]), [], "areas without mapped railway must not create invented tracks");
  const motionNow = Date.parse("2026-11-01T12:00:00Z") / 1000;
  const point = (offset: number, overrides: Partial<MapObservation> = {}): MapObservation => ({
    cod_comercial: "12345", cod_product: 2, cod_origen: "O", cod_destino: "D",
    captured_at: motionNow - 180 + offset, gps_time: motionNow - 180 + offset,
    latitud: 1, longitud: 0, ult_retraso: 0, source: "live", is_estimated: 0, ...overrides,
  });
  const movement = [point(0), point(60, { longitud: 0.01 }), point(180, { longitud: 0.02 })];
  const speed = calculateMapAnalytics(movement, motionNow);
  assert.equal(speed.trains[0]?.averageSpeedKmh, 44.5, "average speed uses total distance / GPS time, not the arithmetic mean of interval speeds");
  assert.equal(speed.trains[0]?.latestSpeedKmh, 33.4);
  assert.equal(speed.trains[0]?.observedMinutes, 3);
  const stopped = calculateMapAnalytics([...movement, point(240, { longitud: 0.02 })], motionNow + 60);
  assert.equal(stopped.trains[0]?.averageSpeedKmh, 33.4, "valid stationary time is included in average speed");
  assert.equal(stopped.trains[0]?.latestSpeedKmh, 0);
  const repeated = calculateMapAnalytics([movement[0]!, movement[1]!, point(70, { gps_time: motionNow - 120, longitud: 0.01 }), movement[2]!], motionNow);
  assert.equal(repeated.trains[0]?.averageSpeedKmh, speed.trains[0]?.averageSpeedKmh);
  assert.equal(repeated.repeatedSamples, 1, "repeated provider GPS timestamps must not create speed samples");
  assert.equal(calculateMapAnalytics(movement.map((row) => ({ ...row, gps_time: row.gps_time! * 1000 })), motionNow).trains[0]?.averageSpeedKmh, speed.trains[0]?.averageSpeedKmh);
  for (const bad of [
    { gps_time: null }, { is_estimated: 1 }, { source: "recovered_snapshot" },
    { latitud: 0, longitud: 0 }, { latitud: 95 }, { gps_time: motionNow - 10000 },
  ]) {
    assert.equal(calculateMapAnalytics([point(0, bad), point(60, bad)], motionNow).validIntervals, 0);
  }
  assert.equal(calculateMapAnalytics([point(0), point(60, { longitud: 30 })], motionNow).validIntervals, 0, "GPS teleports are rejected");
  assert.equal(calculateMapAnalytics([point(0), point(300, { longitud: 0.01 })], motionNow + 120).validIntervals, 0, "long gaps must not be bridged");
  assert.equal(calculateMapAnalytics([point(0), point(60, { cod_destino: "OTHER", longitud: 0.01 })], motionNow).validIntervals, 0, "different services sharing a train number must not be joined");
  assert.equal(calculateMapAnalytics(movement, motionNow + 600).trains[0]?.latestSpeedKmh, null, "stale interval speed must not look live");
  const recurrent = ["11111", "22222"].flatMap((code) => [0, 60, 120, 180].map((offset, i) =>
    point(offset, { cod_comercial: code, latitud: 40.01, longitud: -3.02 + i * 0.002, ult_retraso: i })));
  const ordered = (rows: MapObservation[]) => rows.sort((a, b) => a.captured_at - b.captured_at);
  const hotspots = calculateMapAnalytics(ordered(recurrent), motionNow);
  assert.equal(hotspots.zones.length, 1);
  assert.equal(hotspots.zones[0]?.trainCount, 2);
  assert.equal(hotspots.zones[0]?.gainedDelayMinutes, 6);
  assert.equal(hotspots.zones[0]?.delayIntervals, 6);
  assert.equal(calculateMapAnalytics(recurrent.filter((row) => row.cod_comercial === "11111"), motionNow).zones.length, 0, "one train does not establish a recurrent zone");
  const braking = ["33333", "44444", "55555"].flatMap((code) => [-3.02, -3.005, -3.004].map((lon, i) =>
    point(i * 60, { cod_comercial: code, latitud: 40.01, longitud: lon })));
  const slowdown = calculateMapAnalytics(ordered(braking), motionNow);
  assert.equal(slowdown.zones.length, 0, "braking without added delay must never mark a black zone");
  assert.equal(calculateMapAnalytics(ordered(recurrent.map((row) => ({ ...row, ult_retraso: 30 }))), motionNow).zones.length, 0, "passing through a cell already late does not prove delay was gained there");
  assert.equal(calculateMapAnalytics(ordered(recurrent.map((row) => ({ ...row, ult_retraso: row.ult_retraso - 3 }))), motionNow).zones.length, 0, "losing an early margin without running late is not a delay zone");
  const earlier = recurrent.map((row) => ({ ...row, captured_at: row.captured_at - 10 * 86400, gps_time: row.gps_time! - 10 * 86400 }));
  const earlierAnalysis = calculateMapAnalytics(ordered(earlier), motionNow - 10 * 86400);
  const historicalZones = calculateMapDelayZones([...earlierAnalysis.cells, ...hotspots.cells]);
  assert.equal(historicalZones[0]?.trainCount, 4, "services on different days remain distinct in the full history");
  assert.equal(historicalZones[0]?.gainedDelayMinutes, 12);
  assert.equal(historicalZones[0]?.lastDelayAt, motionNow);
  assert.equal(calculateMapDelayZones([...hotspots.cells, ...hotspots.cells])[0]?.trainCount, 2, "the same service is not counted twice across summary boundaries");
  const boundary = calculateMapAnalytics(ordered(recurrent), motionNow, motionNow - 60);
  assert.equal(boundary.validIntervals, 4, "context before the range preserves crossing intervals without counting earlier ones");
  const normal = Array.from({ length: 40 }, (_, i) => [point(0, { cod_comercial: `normal-${i}`, latitud: 40.01, longitud: -3.01 }), point(60, { cod_comercial: `normal-${i}`, latitud: 40.01, longitud: -3.009 })]).flat();
  assert.equal(calculateMapAnalytics(ordered([...recurrent, ...normal]), motionNow).zones.length, 0, "rare anomalies in busy cells do not establish a recurrent zone");
  assert.equal(calculateMapAnalytics(ordered(recurrent.map((row) => ({ ...row, ult_retraso: row.ult_retraso * 20 }))), motionNow).zones.length, 0, "schedule estimate corrections are not time physically lost in a minute");
  const movementDb = new DB(join(dir, "movement.db"));
  try {
    for (const row of movement) movementDb.insertTrainObservations(null, row.captured_at, [{ train: { ...train, latitud: row.latitud, longitud: row.longitud, gpsTime: row.gps_time }, hash: "gps" }], "live", 0);
    assert.equal(movementDb.getMapAnalytics(motionNow).trains[0]?.averageSpeedKmh, 44.5, "indexed observation queries preserve GPS analytics");
  } finally { movementDb.close(); }

  const delayDbPath = join(dir, "delays.db");
  const delayDb = new DB(delayDbPath);
  const delaySql = new Database(delayDbPath);
  try {
    const midnight = motionNow - 12 * 3600;
    const seedDelays = (end: number, codes = ["11111", "22222"]) => {
      for (const code of codes) for (let i = 0; i < 4; i++) {
        const ts = end - 180 + i * 60;
        delayDb.insertTrainObservations(null, ts, [{ train: { ...train, codComercial: code,
          latitud: 40.41, longitud: -3.695 + i * 0.001, ultRetraso: i, gpsTime: ts }, hash: "delay" }], "live", 0);
      }
    };
    seedDelays(midnight + 60);
    seedDelays(midnight - 10 * 86400 + 60);
    const finishHistory = () => {
      let result = delayDb.getMapAnalytics(midnight + 120);
      for (let i = 0; !result.history.complete && i < 5; i++) result = delayDb.getMapAnalytics(midnight + 120);
      assert.equal(result.history.complete, true);
      return result;
    };
    const delayHistory = finishHistory();
    assert.equal(delayHistory.zones.length, 1);
    assert.equal(delayHistory.zones[0]?.trainCount, 2);
    assert.equal(delayHistory.history.zones[0]?.trainCount, 4, "history includes services older than the recent window without double-counting those crossing midnight");
    assert.equal(delayHistory.history.zones[0]?.gainedDelayMinutes, 12);
    assert.equal(delayHistory.history.zones[0]?.maxDelayMinutes, 3);
    assert.equal(delayHistory.history.zones[0]?.lastDelayAt, midnight + 60);
    assert.ok(delayHistory.history.zones[0]?.railSegments.length);
    delayDb.insertTrainObservations(null, midnight + 60, [{ train: { ...train, codComercial: "11111", gpsTime: midnight + 60 }, hash: "duplicate" }], "live", 0);
    assert.ok(delaySql.query("SELECT 1 FROM map_delay_days WHERE day_epoch=?").get(midnight), "live ingestion keeps the partial-day summary available until its next refresh");
    assert.equal(finishHistory().history.zones[0]?.gainedDelayMinutes, 12, "recomputing historical summaries must not accumulate duplicates");
    delayDb.archiveObservationsBefore(midnight - 86400, join(dir, "delay-archives"), 3);
    delaySql.exec("DELETE FROM map_delay_days");
    assert.equal(finishHistory().history.zones[0]?.gainedDelayMinutes, 12, "full history includes verified cold archives");
    seedDelays(midnight - 10 * 86400 + 60, ["33333"]);
    assert.equal(finishHistory().history.zones[0]?.trainCount, 5, "late observations invalidate the affected historical summary");
    assert.equal(finishHistory().history.zones[0]?.gainedDelayMinutes, 15);
  } finally { delaySql.close(); delayDb.close(); }

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
  await dashboard.generate(24);
  await dashboard.generate(168);
  await ops.generate(168);
  const analytics: RunAnalytics = async (method, ...args) => {
    await Bun.sleep(300);
    if (method === "getHistoricalStatsCustom") return db.getHistoricalStatsCustom(...args as Parameters<DB["getHistoricalStatsCustom"]>) as never;
    throw new Error("unexpected analytics request");
  };
  const meta = db.getCorridorMeta;
  const metrics = db.getOpsMetrics;
  const mapMetrics = db.getMapAnalytics;
  db.getCorridorMeta = () => { throw new Error("cached route must not scan observations"); };
  db.getOpsMetrics = () => { throw new Error("cached route must not compute Ops"); };
  db.getMapAnalytics = () => { throw new Error("dashboard requests must not scan minute observations"); };
  const events = new EventTarget();
  const server = startServer(db, ingestor, dashboard, ops, analytics, events);
  try {
    const base = `http://localhost:${server.port}`;
    const key = async () => (await (await fetch(`${base}/api/auth/request-key`)).json() as { apiKey: string }).apiKey;
    assert.equal((await fetch(`${base}/api/dashboard`)).status, 401);
    const dashboardResponse = await fetch(`${base}/api/dashboard?historyHours=168&limit=1&q=99999`, { headers: { "x-api-key": await key() } });
    assert.equal(dashboardResponse.status, 200);
    const payload = await dashboardResponse.json() as { trains: { total: number; items: Array<{ cod_comercial: string }> }; mapAnalytics: { validIntervals: number; trains: unknown[] } };
    assert.equal(payload.trains.total, 1);
    assert.equal(payload.trains.items[0]?.cod_comercial, "99999");
    assert.equal(payload.mapAnalytics.validIntervals, 0);
    assert.equal(payload.mapAnalytics.trains.length, 0, "missing GPS evidence is never reported as zero speed");
    for (let i = 0; i < 205; i++) {
      db.upsertCurrentTrain({
        train: { ...train, codComercial: `map-${String(i).padStart(3, "0")}`, ultRetraso: i },
        nowEpoch: now, hash: `map-${i}`, snapshotAt: null,
      });
    }
    type MapTrain = { cod_comercial: string; latitud: number; longitud: number; ult_retraso: number; product_name: string; gps_time: number | null; last_seen_at: number; cod_est_sig: string | null; des_corridor: string | null };
    const readMap = async (params: string) => {
      const response = await fetch(`${base}/api/dashboard?${params}`, { headers: { "x-api-key": await key(), "accept-language": "en" } });
      assert.equal(response.status, 200);
      return response.json() as Promise<{ mapTrains: MapTrain[]; trains: { total: number; items: MapTrain[] } }>;
    };
    const allMap = await readMap("q=map-&limit=1&offset=1");
    assert.equal(allMap.trains.items.length, 1);
    assert.equal(allMap.trains.items[0]?.cod_comercial, "map-203", "table retains delay sorting and pagination");
    assert.equal(allMap.trains.total, 205);
    assert.equal(allMap.mapTrains.length, 205, "map must include all filtered trains beyond the table and API page limits");
    assert.ok(allMap.mapTrains.some((item) => item.cod_comercial === "map-000"));
    assert.ok(allMap.mapTrains.every((item) => item.product_name), "map uses the existing product labels");
    const trainDetail = allMap.mapTrains.find((item) => item.cod_comercial === "map-000")!;
    assert.equal(trainDetail.gps_time, train.gpsTime, "train details retain the provider GPS time separately from ingestion time");
    assert.equal(trainDetail.last_seen_at, now);
    assert.equal(trainDetail.cod_est_sig, train.codEstSig, "train details retain the next station");
    assert.equal(trainDetail.des_corridor, train.desCorridor);
    const delayedMap = await readMap("q=map-&minDelay=200");
    assert.equal(delayedMap.mapTrains.length, 5);
    assert.ok(delayedMap.mapTrains.every((item) => item.ult_retraso >= 200), "map shares the table filters");
    db.upsertCurrentTrain({ train: { ...train, codComercial: "map-000", latitud: 41, longitud: 2 }, nowEpoch: now + 1, hash: "moved", snapshotAt: null });
    const movedMap = await readMap("q=map-000");
    assert.equal(movedMap.mapTrains[0]?.latitud, 41, "map reads the latest position without waiting for historical caches");
    assert.equal(movedMap.mapTrains[0]?.longitud, 2);
    sql.query("DELETE FROM trains_current WHERE cod_comercial LIKE 'map-%'").run();
    assert.deepEqual((await readMap("q=map-")).mapTrains, [], "expired trains disappear from the map");
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
    const coldPayload = await cold.json() as { historicalPending: boolean; trains: { total: number }; mapTrains: MapTrain[] };
    assert.equal(coldPayload.historicalPending, true);
    assert.equal(coldPayload.trains.total, 1);
    assert.equal(coldPayload.mapTrains.length, 1, "live map remains available with a cold historical cache");
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
    db.getMapAnalytics = mapMetrics;
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
  console.log("OK: recovery, retention, atomic ingestion, archives, hourly boundaries, cache, live map, auth, concurrent API and SSE");
} finally {
  Object.assign(config, originalConfig);
  globalThis.fetch = originalFetch;
  sql.close();
  db.close();
  rmSync(dir, { recursive: true, force: true });
}
