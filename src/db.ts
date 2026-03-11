import { Database } from "bun:sqlite";
import { dirname } from "node:path";
import { mkdirSync } from "node:fs";
import type {
  CurrentTrainRow,
  DashboardOverview,
  DelayBuckets,
  IngestionRun,
  NormalizedTrain,
  ProductMetric,
  StationRecord,
} from "./types";
import { getProductName } from "./products";
import { getDelayBucketFlags } from "./utils";

interface UpsertCurrentArgs {
  train: NormalizedTrain;
  nowEpoch: number;
  hash: string;
  snapshotAt: number | null;
}

interface DailyStatsArgs {
  train: NormalizedTrain;
  day: string;
  nowEpoch: number;
  distanceKm: number;
}

interface TrainListArgs {
  query: string | null;
  minDelay: number | null;
  limit: number;
  offset: number;
}

interface IngestionBatchArgs {
  fetchedAt: number;
  source: string;
  providerUpdatedAt: string | null;
  trainCount: number;
  payloadHash: string | null;
}

interface ObservationInsertItem {
  train: NormalizedTrain;
  hash: string;
}

export class DB {
  private readonly db: Database;

  constructor(dbPath: string) {
    mkdirSync(dirname(dbPath), { recursive: true });
    this.db = new Database(dbPath, { create: true, strict: true });
    this.configure();
    this.createSchema();
  }

  private configure() {
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA temp_store = MEMORY;
      PRAGMA cache_size = -64000;
      PRAGMA foreign_keys = ON;
    `);
  }

  private createSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS app_state (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS ingestion_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fetched_at INTEGER NOT NULL,
        source TEXT NOT NULL,
        success INTEGER NOT NULL,
        train_count INTEGER NOT NULL DEFAULT 0,
        skipped INTEGER NOT NULL DEFAULT 0,
        error TEXT,
        provider_updated_at TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_ingestion_fetched_at ON ingestion_runs(fetched_at DESC);

      CREATE TABLE IF NOT EXISTS ingestion_batches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fetched_at INTEGER NOT NULL,
        source TEXT NOT NULL,
        provider_updated_at TEXT,
        train_count INTEGER NOT NULL DEFAULT 0,
        payload_hash TEXT,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_batches_fetched_at ON ingestion_batches(fetched_at DESC);

      CREATE TABLE IF NOT EXISTS stations (
        code TEXT PRIMARY KEY,
        name TEXT,
        locality TEXT,
        province TEXT,
        accessible INTEGER,
        attended INTEGER,
        correspondences TEXT,
        level TEXT,
        lat REAL,
        lon REAL,
        updated_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS trains_current (
        cod_comercial TEXT PRIMARY KEY,
        cod_product INTEGER NOT NULL,
        cod_origen TEXT,
        cod_destino TEXT,
        cod_est_ant TEXT,
        cod_est_sig TEXT,
        hora_llegada_sig_est TEXT,
        des_corridor TEXT,
        accesible INTEGER NOT NULL,
        ult_retraso INTEGER NOT NULL,
        latitud REAL NOT NULL,
        longitud REAL NOT NULL,
        gps_time INTEGER,
        p TEXT,
        mat TEXT,
        first_seen_at INTEGER NOT NULL,
        last_seen_at INTEGER NOT NULL,
        last_payload_hash TEXT,
        last_snapshot_at INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_current_product ON trains_current(cod_product);
      CREATE INDEX IF NOT EXISTS idx_current_last_seen ON trains_current(last_seen_at DESC);
      CREATE INDEX IF NOT EXISTS idx_current_delay ON trains_current(ult_retraso DESC);

      CREATE TABLE IF NOT EXISTS train_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cod_comercial TEXT NOT NULL,
        captured_at INTEGER NOT NULL,
        cod_product INTEGER NOT NULL,
        cod_origen TEXT,
        cod_destino TEXT,
        cod_est_ant TEXT,
        cod_est_sig TEXT,
        hora_llegada_sig_est TEXT,
        des_corridor TEXT,
        accesible INTEGER NOT NULL,
        ult_retraso INTEGER NOT NULL,
        latitud REAL NOT NULL,
        longitud REAL NOT NULL,
        gps_time INTEGER,
        p TEXT,
        mat TEXT,
        hash TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_snapshots_train_time ON train_snapshots(cod_comercial, captured_at DESC);
      CREATE INDEX IF NOT EXISTS idx_snapshots_time ON train_snapshots(captured_at DESC);

      CREATE TABLE IF NOT EXISTS train_observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id INTEGER,
        cod_comercial TEXT NOT NULL,
        captured_at INTEGER NOT NULL,
        cod_product INTEGER NOT NULL,
        cod_origen TEXT,
        cod_destino TEXT,
        cod_est_ant TEXT,
        cod_est_sig TEXT,
        hora_llegada_sig_est TEXT,
        des_corridor TEXT,
        accesible INTEGER NOT NULL,
        ult_retraso INTEGER NOT NULL,
        latitud REAL NOT NULL,
        longitud REAL NOT NULL,
        gps_time INTEGER,
        p TEXT,
        mat TEXT,
        hash TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'live',
        is_estimated INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY(batch_id) REFERENCES ingestion_batches(id) ON DELETE SET NULL
      );
      CREATE INDEX IF NOT EXISTS idx_observations_train_time ON train_observations(cod_comercial, captured_at DESC);
      CREATE INDEX IF NOT EXISTS idx_observations_time ON train_observations(captured_at DESC);
      CREATE INDEX IF NOT EXISTS idx_observations_batch ON train_observations(batch_id);
      CREATE UNIQUE INDEX IF NOT EXISTS idx_observations_unique ON train_observations(cod_comercial, captured_at, source);

      CREATE TABLE IF NOT EXISTS train_daily_stats (
        day TEXT NOT NULL,
        cod_comercial TEXT NOT NULL,
        cod_product INTEGER NOT NULL,
        cod_origen TEXT,
        cod_destino TEXT,
        des_corridor TEXT,
        observations INTEGER NOT NULL DEFAULT 0,
        ahead_count INTEGER NOT NULL DEFAULT 0,
        on_time_count INTEGER NOT NULL DEFAULT 0,
        mild_count INTEGER NOT NULL DEFAULT 0,
        medium_count INTEGER NOT NULL DEFAULT 0,
        severe_count INTEGER NOT NULL DEFAULT 0,
        avg_delay REAL NOT NULL DEFAULT 0,
        max_delay INTEGER NOT NULL DEFAULT 0,
        min_delay INTEGER NOT NULL DEFAULT 0,
        total_distance_km REAL NOT NULL DEFAULT 0,
        first_seen_at INTEGER NOT NULL,
        last_seen_at INTEGER NOT NULL,
        PRIMARY KEY (day, cod_comercial)
      );
      CREATE INDEX IF NOT EXISTS idx_daily_day ON train_daily_stats(day DESC);
      CREATE INDEX IF NOT EXISTS idx_daily_product_day ON train_daily_stats(cod_product, day DESC);
    `);
  }

  close() {
    this.db.close();
  }

  getState(key: string): string | null {
    const row = this.db
      .query("SELECT value FROM app_state WHERE key = ?")
      .get(key) as { value: string } | null;

    return row?.value ?? null;
  }

  setState(key: string, value: string) {
    const now = Math.floor(Date.now() / 1000);
    this.db
      .query(
        `
        INSERT INTO app_state (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
          value = excluded.value,
          updated_at = excluded.updated_at
      `,
      )
      .run(key, value, now);
  }

  recordIngestionRun(input: IngestionRun) {
    this.db
      .query(
        `
      INSERT INTO ingestion_runs
      (fetched_at, source, success, train_count, skipped, error, provider_updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      `,
      )
      .run(
        input.fetchedAt,
        input.source,
        input.success,
        input.trainCount,
        input.skipped,
        input.error,
        input.providerUpdatedAt,
      );
  }

  createIngestionBatch(args: IngestionBatchArgs): number {
    const result = this.db
      .query(
        `
      INSERT INTO ingestion_batches
      (fetched_at, source, provider_updated_at, train_count, payload_hash, created_at)
      VALUES (?, ?, ?, ?, ?, ?)
      `,
      )
      .run(
        args.fetchedAt,
        args.source,
        args.providerUpdatedAt,
        args.trainCount,
        args.payloadHash,
        args.fetchedAt,
      );

    return Number(result.lastInsertRowid);
  }

  insertTrainObservations(
    batchId: number | null,
    capturedAt: number,
    items: ObservationInsertItem[],
    source: string,
    isEstimated: number,
  ) {
    const run = this.db.transaction((rows: ObservationInsertItem[]) => {
      const query = this.db.query(
        `
      INSERT OR IGNORE INTO train_observations (
        batch_id,
        cod_comercial,
        captured_at,
        cod_product,
        cod_origen,
        cod_destino,
        cod_est_ant,
        cod_est_sig,
        hora_llegada_sig_est,
        des_corridor,
        accesible,
        ult_retraso,
        latitud,
        longitud,
        gps_time,
        p,
        mat,
        hash,
        source,
        is_estimated
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      );

      for (const row of rows) {
        const train = row.train;
        query.run(
          batchId,
          train.codComercial,
          capturedAt,
          train.codProduct,
          train.codOrigen,
          train.codDestino,
          train.codEstAnt,
          train.codEstSig,
          train.horaLlegadaSigEst,
          train.desCorridor,
          train.accesible,
          train.ultRetraso,
          train.latitud,
          train.longitud,
          train.gpsTime,
          train.p,
          train.mat,
          row.hash,
          source,
          isEstimated,
        );
      }
    });

    run(items);
  }

  getCurrentTrain(codComercial: string): CurrentTrainRow | null {
    return (
      (this.db
        .query(
          `
        SELECT *
        FROM trains_current
        WHERE cod_comercial = ?
      `,
        )
        .get(codComercial) as CurrentTrainRow | null) ?? null
    );
  }

  upsertCurrentTrain(args: UpsertCurrentArgs) {
    const { train, nowEpoch, hash, snapshotAt } = args;

    this.db
      .query(
        `
      INSERT INTO trains_current (
        cod_comercial,
        cod_product,
        cod_origen,
        cod_destino,
        cod_est_ant,
        cod_est_sig,
        hora_llegada_sig_est,
        des_corridor,
        accesible,
        ult_retraso,
        latitud,
        longitud,
        gps_time,
        p,
        mat,
        first_seen_at,
        last_seen_at,
        last_payload_hash,
        last_snapshot_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(cod_comercial) DO UPDATE SET
        cod_product = excluded.cod_product,
        cod_origen = excluded.cod_origen,
        cod_destino = excluded.cod_destino,
        cod_est_ant = excluded.cod_est_ant,
        cod_est_sig = excluded.cod_est_sig,
        hora_llegada_sig_est = excluded.hora_llegada_sig_est,
        des_corridor = excluded.des_corridor,
        accesible = excluded.accesible,
        ult_retraso = excluded.ult_retraso,
        latitud = excluded.latitud,
        longitud = excluded.longitud,
        gps_time = excluded.gps_time,
        p = excluded.p,
        mat = excluded.mat,
        last_seen_at = excluded.last_seen_at,
        last_payload_hash = excluded.last_payload_hash,
        last_snapshot_at = COALESCE(excluded.last_snapshot_at, trains_current.last_snapshot_at)
      `,
      )
      .run(
        train.codComercial,
        train.codProduct,
        train.codOrigen,
        train.codDestino,
        train.codEstAnt,
        train.codEstSig,
        train.horaLlegadaSigEst,
        train.desCorridor,
        train.accesible,
        train.ultRetraso,
        train.latitud,
        train.longitud,
        train.gpsTime,
        train.p,
        train.mat,
        nowEpoch,
        nowEpoch,
        hash,
        snapshotAt,
      );
  }

  insertSnapshot(train: NormalizedTrain, capturedAt: number, hash: string) {
    this.db
      .query(
        `
      INSERT INTO train_snapshots (
        cod_comercial,
        captured_at,
        cod_product,
        cod_origen,
        cod_destino,
        cod_est_ant,
        cod_est_sig,
        hora_llegada_sig_est,
        des_corridor,
        accesible,
        ult_retraso,
        latitud,
        longitud,
        gps_time,
        p,
        mat,
        hash
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      )
      .run(
        train.codComercial,
        capturedAt,
        train.codProduct,
        train.codOrigen,
        train.codDestino,
        train.codEstAnt,
        train.codEstSig,
        train.horaLlegadaSigEst,
        train.desCorridor,
        train.accesible,
        train.ultRetraso,
        train.latitud,
        train.longitud,
        train.gpsTime,
        train.p,
        train.mat,
        hash,
      );
  }

  upsertDailyStats(args: DailyStatsArgs) {
    const { train, day, nowEpoch, distanceKm } = args;
    const flags = getDelayBucketFlags(train.ultRetraso);

    this.db
      .query(
        `
      INSERT INTO train_daily_stats (
        day,
        cod_comercial,
        cod_product,
        cod_origen,
        cod_destino,
        des_corridor,
        observations,
        ahead_count,
        on_time_count,
        mild_count,
        medium_count,
        severe_count,
        avg_delay,
        max_delay,
        min_delay,
        total_distance_km,
        first_seen_at,
        last_seen_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(day, cod_comercial) DO UPDATE SET
        cod_product = excluded.cod_product,
        cod_origen = excluded.cod_origen,
        cod_destino = excluded.cod_destino,
        des_corridor = excluded.des_corridor,
        observations = train_daily_stats.observations + 1,
        ahead_count = train_daily_stats.ahead_count + excluded.ahead_count,
        on_time_count = train_daily_stats.on_time_count + excluded.on_time_count,
        mild_count = train_daily_stats.mild_count + excluded.mild_count,
        medium_count = train_daily_stats.medium_count + excluded.medium_count,
        severe_count = train_daily_stats.severe_count + excluded.severe_count,
        avg_delay = ROUND(
          ((train_daily_stats.avg_delay * train_daily_stats.observations) + excluded.avg_delay)
          / (train_daily_stats.observations + 1),
          2
        ),
        max_delay = MAX(train_daily_stats.max_delay, excluded.max_delay),
        min_delay = MIN(train_daily_stats.min_delay, excluded.min_delay),
        total_distance_km = train_daily_stats.total_distance_km + excluded.total_distance_km,
        last_seen_at = excluded.last_seen_at
      `,
      )
      .run(
        day,
        train.codComercial,
        train.codProduct,
        train.codOrigen,
        train.codDestino,
        train.desCorridor,
        1,
        flags.ahead,
        flags.onTime,
        flags.mild,
        flags.medium,
        flags.severe,
        train.ultRetraso,
        train.ultRetraso,
        train.ultRetraso,
        distanceKm,
        nowEpoch,
        nowEpoch,
      );
  }

  upsertStations(stations: StationRecord[], updatedAt: number) {
    const run = this.db.transaction((items: StationRecord[]) => {
      const query = this.db.query(
        `
          INSERT INTO stations (
            code,
            name,
            locality,
            province,
            accessible,
            attended,
            correspondences,
            level,
            lat,
            lon,
            updated_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(code) DO UPDATE SET
            name = excluded.name,
            locality = excluded.locality,
            province = excluded.province,
            accessible = excluded.accessible,
            attended = excluded.attended,
            correspondences = excluded.correspondences,
            level = excluded.level,
            lat = excluded.lat,
            lon = excluded.lon,
            updated_at = excluded.updated_at
        `,
      );

      for (const station of items) {
        query.run(
          station.code,
          station.name,
          station.locality,
          station.province,
          station.accessible,
          station.attended,
          station.correspondences,
          station.level,
          station.lat,
          station.lon,
          updatedAt,
        );
      }
    });

    run(stations);
  }

  deleteStaleCurrentTrains(cutoffEpoch: number): number {
    const result = this.db
      .query(`DELETE FROM trains_current WHERE last_seen_at < ?`)
      .run(cutoffEpoch);

    return result.changes;
  }

  cleanupSnapshots(cutoffEpoch: number): number {
    const result = this.db
      .query(`DELETE FROM train_snapshots WHERE captured_at < ?`)
      .run(cutoffEpoch);

    return result.changes;
  }

  cleanupObservations(cutoffEpoch: number): number {
    const result = this.db
      .query(`DELETE FROM train_observations WHERE captured_at < ?`)
      .run(cutoffEpoch);

    return result.changes;
  }

  cleanupBatches(cutoffEpoch: number): number {
    const result = this.db
      .query(`DELETE FROM ingestion_batches WHERE fetched_at < ?`)
      .run(cutoffEpoch);

    return result.changes;
  }

  cleanupIngestionRuns(cutoffEpoch: number): number {
    const result = this.db
      .query(`DELETE FROM ingestion_runs WHERE fetched_at < ?`)
      .run(cutoffEpoch);

    return result.changes;
  }

  optimize() {
    this.db.exec(`PRAGMA optimize;`);
    this.db.exec(`PRAGMA wal_checkpoint(PASSIVE);`);
  }

  getOverview(): DashboardOverview {
    const row = this.db
      .query(
        `
      SELECT
        COUNT(*) AS activeTrains,
        COALESCE(ROUND(AVG(ult_retraso), 2), 0) AS avgDelay,
        COALESCE(MAX(ult_retraso), 0) AS maxDelay,
        COALESCE(SUM(CASE WHEN ult_retraso > 15 THEN 1 ELSE 0 END), 0) AS delayedOver15,
        COALESCE(SUM(CASE WHEN ult_retraso > 60 THEN 1 ELSE 0 END), 0) AS severeOver60,
        COALESCE(SUM(CASE WHEN accesible = 1 THEN 1 ELSE 0 END), 0) AS accessibleCount,
        COALESCE(SUM(CASE WHEN ult_retraso = 0 THEN 1 ELSE 0 END), 0) AS onTimeCount,
        COALESCE(SUM(CASE WHEN ult_retraso < 0 THEN 1 ELSE 0 END), 0) AS aheadCount,
        COALESCE(SUM(CASE WHEN ult_retraso IS NOT NULL THEN 1 ELSE 0 END), 0) AS withDataCount,
        MAX(last_seen_at) AS lastSeenAt
      FROM trains_current
      `,
      )
      .get() as
      | {
          activeTrains: number;
          avgDelay: number;
          maxDelay: number;
          delayedOver15: number;
          severeOver60: number;
          accessibleCount: number;
          onTimeCount: number;
          aheadCount: number;
          withDataCount: number;
          lastSeenAt: number | null;
        }
      | undefined;

    return {
      activeTrains: row?.activeTrains ?? 0,
      avgDelay: row?.avgDelay ?? 0,
      maxDelay: row?.maxDelay ?? 0,
      delayedOver15: row?.delayedOver15 ?? 0,
      severeOver60: row?.severeOver60 ?? 0,
      accessibleCount: row?.accessibleCount ?? 0,
      onTimeCount: row?.onTimeCount ?? 0,
      aheadCount: row?.aheadCount ?? 0,
      withDataCount: row?.withDataCount ?? 0,
      lastSeenAt: row?.lastSeenAt ?? null,
    };
  }

  getDelayBuckets(): DelayBuckets {
    const row = this.db
      .query(
        `
      SELECT
        COALESCE(SUM(CASE WHEN ult_retraso < 0 THEN 1 ELSE 0 END), 0) AS ahead,
        COALESCE(SUM(CASE WHEN ult_retraso = 0 THEN 1 ELSE 0 END), 0) AS onTime,
        COALESCE(SUM(CASE WHEN ult_retraso BETWEEN 1 AND 15 THEN 1 ELSE 0 END), 0) AS mild,
        COALESCE(SUM(CASE WHEN ult_retraso BETWEEN 16 AND 60 THEN 1 ELSE 0 END), 0) AS medium,
        COALESCE(SUM(CASE WHEN ult_retraso > 60 THEN 1 ELSE 0 END), 0) AS severe
      FROM trains_current
      `,
      )
      .get() as DelayBuckets | undefined;

    return {
      ahead: row?.ahead ?? 0,
      onTime: row?.onTime ?? 0,
      mild: row?.mild ?? 0,
      medium: row?.medium ?? 0,
      severe: row?.severe ?? 0,
    };
  }

  getProductMetrics(): ProductMetric[] {
    const rows = this.db
      .query(
        `
      SELECT
        cod_product,
        COUNT(*) AS count,
        COALESCE(ROUND(AVG(ult_retraso), 2), 0) AS avg_delay,
        COALESCE(MAX(ult_retraso), 0) AS max_delay
      FROM trains_current
      GROUP BY cod_product
      ORDER BY count DESC, avg_delay DESC
      `,
      )
      .all() as Array<{
      cod_product: number;
      count: number;
      avg_delay: number;
      max_delay: number;
    }>;

    return rows.map((row) => ({
      codProduct: row.cod_product,
      productName: getProductName(row.cod_product),
      count: row.count,
      avgDelay: row.avg_delay,
      maxDelay: row.max_delay,
    }));
  }

  getTopCorridors(limit: number) {
    return this.db
      .query(
        `
      SELECT
        COALESCE(des_corridor, 'Sin corredor') AS corridor,
        COUNT(*) AS train_count,
        COALESCE(ROUND(AVG(ult_retraso), 2), 0) AS avg_delay,
        COALESCE(MAX(ult_retraso), 0) AS max_delay
      FROM trains_current
      GROUP BY des_corridor
      ORDER BY train_count DESC, avg_delay DESC
      LIMIT ?
      `,
      )
      .all(limit);
  }

  listTrains(args: TrainListArgs) {
    const query = args.query ? `%${args.query}%` : null;

    return this.db
      .query(
        `
      SELECT
        t.cod_comercial,
        t.cod_product,
        t.cod_origen,
        t.cod_destino,
        t.cod_est_ant,
        t.cod_est_sig,
        t.hora_llegada_sig_est,
        t.des_corridor,
        t.accesible,
        t.ult_retraso,
        t.latitud,
        t.longitud,
        t.gps_time,
        t.p,
        t.mat,
        t.first_seen_at,
        t.last_seen_at,
        so.name AS origin_name,
        sd.name AS destination_name,
        sa.name AS previous_station_name,
        ss.name AS next_station_name
      FROM trains_current t
      LEFT JOIN stations so ON so.code = t.cod_origen
      LEFT JOIN stations sd ON sd.code = t.cod_destino
      LEFT JOIN stations sa ON sa.code = t.cod_est_ant
      LEFT JOIN stations ss ON ss.code = t.cod_est_sig
      WHERE
        (?1 IS NULL OR
          t.cod_comercial LIKE ?1 OR
          t.des_corridor LIKE ?1 OR
          so.name LIKE ?1 OR
          sd.name LIKE ?1)
        AND (?2 IS NULL OR t.ult_retraso >= ?2)
      ORDER BY t.ult_retraso DESC, t.last_seen_at DESC
      LIMIT ?3 OFFSET ?4
      `,
      )
      .all(query, args.minDelay, args.limit, args.offset);
  }

  countTrains(query: string | null, minDelay: number | null): number {
    const pattern = query ? `%${query}%` : null;
    const row = this.db
      .query(
        `
      SELECT COUNT(*) AS total
      FROM trains_current t
      LEFT JOIN stations so ON so.code = t.cod_origen
      LEFT JOIN stations sd ON sd.code = t.cod_destino
      WHERE
        (?1 IS NULL OR
          t.cod_comercial LIKE ?1 OR
          t.des_corridor LIKE ?1 OR
          so.name LIKE ?1 OR
          sd.name LIKE ?1)
        AND (?2 IS NULL OR t.ult_retraso >= ?2)
      `,
      )
      .get(pattern, minDelay) as { total: number } | undefined;

    return row?.total ?? 0;
  }

  getTrainHistory(codComercial: string, hours: number) {
    const since = Math.floor(Date.now() / 1000) - Math.max(1, hours) * 3600;

    const snapshots = this.db
      .query(
        `
      SELECT
        captured_at,
        ult_retraso,
        latitud,
        longitud,
        cod_est_ant,
        cod_est_sig,
        hora_llegada_sig_est,
        hash
      FROM train_snapshots
      WHERE cod_comercial = ? AND captured_at >= ?
      ORDER BY captured_at ASC
      `,
      )
      .all(codComercial, since);

    const daily = this.db
      .query(
        `
      SELECT
        day,
        observations,
        avg_delay,
        max_delay,
        min_delay,
        total_distance_km,
        ahead_count,
        on_time_count,
        mild_count,
        medium_count,
        severe_count
      FROM train_daily_stats
      WHERE cod_comercial = ?
      ORDER BY day DESC
      LIMIT 30
      `,
      )
      .all(codComercial);

    const observations = this.db
      .query(
        `
      SELECT
        captured_at,
        ult_retraso,
        latitud,
        longitud,
        cod_est_ant,
        cod_est_sig,
        hora_llegada_sig_est,
        source,
        is_estimated
      FROM train_observations
      WHERE cod_comercial = ? AND captured_at >= ?
      ORDER BY captured_at ASC
      LIMIT 20000
      `,
      )
      .all(codComercial, since);

    return { snapshots, observations, daily };
  }

  recoverObservationsFromSnapshots(sinceEpoch: number): number {
    const result = this.db
      .query(
        `
      INSERT OR IGNORE INTO train_observations (
        batch_id,
        cod_comercial,
        captured_at,
        cod_product,
        cod_origen,
        cod_destino,
        cod_est_ant,
        cod_est_sig,
        hora_llegada_sig_est,
        des_corridor,
        accesible,
        ult_retraso,
        latitud,
        longitud,
        gps_time,
        p,
        mat,
        hash,
        source,
        is_estimated
      )
      SELECT
        NULL AS batch_id,
        s.cod_comercial,
        s.captured_at,
        s.cod_product,
        s.cod_origen,
        s.cod_destino,
        s.cod_est_ant,
        s.cod_est_sig,
        s.hora_llegada_sig_est,
        s.des_corridor,
        s.accesible,
        s.ult_retraso,
        s.latitud,
        s.longitud,
        s.gps_time,
        s.p,
        s.mat,
        s.hash,
        'recovered_snapshot' AS source,
        1 AS is_estimated
      FROM train_snapshots s
      WHERE s.captured_at >= ?
      `,
      )
      .run(sinceEpoch);

    return result.changes;
  }

  getHistoryCoverage(hours: number, expectedIntervalSeconds: number) {
    const now = Math.floor(Date.now() / 1000);
    const safeHours = Math.max(1, Math.min(720, Math.trunc(hours)));
    const since = now - safeHours * 3600;

    const observations = this.db
      .query(
        `
      SELECT
        COUNT(*) AS total,
        COUNT(DISTINCT cod_comercial) AS unique_trains,
        COALESCE(SUM(is_estimated), 0) AS estimated_total,
        MIN(captured_at) AS min_t,
        MAX(captured_at) AS max_t
      FROM train_observations
      WHERE captured_at >= ?
      `,
      )
      .get(since) as
      | {
          total: number;
          unique_trains: number;
          estimated_total: number;
          min_t: number | null;
          max_t: number | null;
        }
      | undefined;

    const snapshots = this.db
      .query(
        `
      SELECT
        COUNT(*) AS total,
        MIN(captured_at) AS min_t,
        MAX(captured_at) AS max_t
      FROM train_snapshots
      WHERE captured_at >= ?
      `,
      )
      .get(since) as
      | {
          total: number;
          min_t: number | null;
          max_t: number | null;
        }
      | undefined;

    const runs = this.db
      .query(
        `
      SELECT
        COUNT(*) AS total,
        COALESCE(SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END), 0) AS successful,
        COALESCE(SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END), 0) AS failed,
        COALESCE(SUM(CASE WHEN skipped = 1 THEN 1 ELSE 0 END), 0) AS skipped,
        MIN(fetched_at) AS min_t,
        MAX(fetched_at) AS max_t
      FROM ingestion_runs
      WHERE fetched_at >= ?
      `,
      )
      .get(since) as
      | {
          total: number;
          successful: number;
          failed: number;
          skipped: number;
          min_t: number | null;
          max_t: number | null;
        }
      | undefined;

    const gapRow = this.db
      .query(
        `
      SELECT
        COALESCE(MAX(gap_sec), 0) AS max_gap_sec,
        COALESCE(SUM(CASE WHEN gap_sec > (?1 * 2) THEN 1 ELSE 0 END), 0) AS gaps_over_2x
      FROM (
        SELECT fetched_at - LAG(fetched_at) OVER (ORDER BY fetched_at) AS gap_sec
        FROM ingestion_runs
        WHERE fetched_at >= ?2 AND success = 1
      ) g
      WHERE gap_sec IS NOT NULL
      `,
      )
      .get(expectedIntervalSeconds, since) as
      | {
          max_gap_sec: number;
          gaps_over_2x: number;
        }
      | undefined;

    const expectedRuns = Math.floor((safeHours * 3600) / Math.max(1, expectedIntervalSeconds));
    const observedRuns = runs?.successful ?? 0;
    const estimatedMissingRuns = Math.max(0, expectedRuns - observedRuns);

    return {
      hours: safeHours,
      sinceEpoch: since,
      nowEpoch: now,
      expectedRuns,
      observedRuns,
      estimatedMissingRuns,
      observations: {
        total: observations?.total ?? 0,
        uniqueTrains: observations?.unique_trains ?? 0,
        estimatedTotal: observations?.estimated_total ?? 0,
        minTs: observations?.min_t ?? null,
        maxTs: observations?.max_t ?? null,
      },
      snapshots: {
        total: snapshots?.total ?? 0,
        minTs: snapshots?.min_t ?? null,
        maxTs: snapshots?.max_t ?? null,
      },
      runs: {
        total: runs?.total ?? 0,
        successful: runs?.successful ?? 0,
        failed: runs?.failed ?? 0,
        skipped: runs?.skipped ?? 0,
        minTs: runs?.min_t ?? null,
        maxTs: runs?.max_t ?? null,
      },
      gapReport: {
        maxGapSec: gapRow?.max_gap_sec ?? 0,
        gapsOver2x: gapRow?.gaps_over_2x ?? 0,
      },
    };
  }

  getRecentRuns(limit: number) {
    return this.db
      .query(
        `
      SELECT
        fetched_at,
        source,
        success,
        train_count,
        skipped,
        error,
        provider_updated_at
      FROM ingestion_runs
      ORDER BY fetched_at DESC
      LIMIT ?
      `,
      )
      .all(limit);
  }

  getTodayAggregate() {
    const day = new Date().toISOString().slice(0, 10);

    const row = this.db
      .query(
        `
      SELECT
        COUNT(*) AS unique_trains,
        COALESCE(SUM(observations), 0) AS observations,
        COALESCE(ROUND(SUM(avg_delay * observations) / NULLIF(SUM(observations), 0), 2), 0) AS weighted_avg_delay,
        COALESCE(MAX(max_delay), 0) AS peak_delay,
        COALESCE(ROUND(SUM(total_distance_km), 2), 0) AS km_tracked
      FROM train_daily_stats
      WHERE day = ?
      `,
      )
      .get(day) as
      | {
          unique_trains: number;
          observations: number;
          weighted_avg_delay: number;
          peak_delay: number;
          km_tracked: number;
        }
      | undefined;

    return {
      day,
      uniqueTrains: row?.unique_trains ?? 0,
      observations: row?.observations ?? 0,
      weightedAvgDelay: row?.weighted_avg_delay ?? 0,
      peakDelay: row?.peak_delay ?? 0,
      kmTracked: row?.km_tracked ?? 0,
    };
  }

  getTodayTypeInsights() {
    const day = new Date().toISOString().slice(0, 10);
    const dayStartEpoch = Math.floor(Date.parse(`${day}T00:00:00Z`) / 1000);

    const problematic = this.db
      .query(
        `
      SELECT
        cod_product,
        COALESCE(SUM(CASE WHEN ult_retraso > 0 THEN ult_retraso ELSE 0 END), 0) AS accumulated_delay_minutes,
        COUNT(*) AS observations,
        COUNT(DISTINCT cod_comercial) AS affected_trains
      FROM train_observations
      WHERE captured_at >= ?
      GROUP BY cod_product
      ORDER BY accumulated_delay_minutes DESC, observations DESC
      LIMIT 1
      `,
      )
      .get(dayStartEpoch) as
      | {
          cod_product: number;
          accumulated_delay_minutes: number;
          observations: number;
          affected_trains: number;
        }
      | undefined;

    const volume = this.db
      .query(
        `
      SELECT
        cod_product,
        COUNT(*) AS trains,
        COALESCE(ROUND(SUM(total_distance_km), 2), 0) AS total_km
      FROM train_daily_stats
      WHERE day = ?
      GROUP BY cod_product
      ORDER BY trains DESC, total_km DESC
      LIMIT 1
      `,
      )
      .get(day) as
      | {
          cod_product: number;
          trains: number;
          total_km: number;
        }
      | undefined;

    return {
      problematic: problematic
        ? {
            codProduct: problematic.cod_product,
            accumulatedDelayMinutes: problematic.accumulated_delay_minutes,
            observations: problematic.observations,
            affectedTrains: problematic.affected_trains,
          }
        : null,
      volume: volume
        ? {
            codProduct: volume.cod_product,
            trains: volume.trains,
            totalKm: volume.total_km,
          }
        : null,
    };
  }

  getHistoricalStats(hours: number) {
    const now = Math.floor(Date.now() / 1000);
    const safeHours = Math.max(24, Math.min(24 * 30, Math.trunc(hours)));
    const since = now - safeHours * 3600;

    return this.getHistoricalStatsByRange(since, now);
  }

  getHistoricalStatsCustom(sinceEpoch: number, untilEpoch: number) {
    const now = Math.floor(Date.now() / 1000);
    const safeSince = Math.max(0, Math.min(sinceEpoch, untilEpoch));
    const safeUntil = Math.max(safeSince + 1, Math.min(untilEpoch, now));

    return this.getHistoricalStatsByRange(safeSince, safeUntil);
  }

  private getHistoricalStatsByRange(since: number, until: number) {
    const safeHours = Math.max(1, Math.round((until - since) / 3600));
    const sinceDay = new Date(since * 1000).toISOString().slice(0, 10);
    const untilDay = new Date(until * 1000).toISOString().slice(0, 10);

    const summaryRow = this.db
      .query(
        `
      SELECT
        COUNT(*) AS observations,
        COUNT(DISTINCT cod_comercial) AS unique_trains,
        COALESCE(ROUND(AVG(ult_retraso), 2), 0) AS avg_delay,
        COALESCE(MAX(ult_retraso), 0) AS max_delay,
        COALESCE(SUM(CASE WHEN ult_retraso = 0 THEN 1 ELSE 0 END), 0) AS on_time_count,
        COALESCE(SUM(CASE WHEN ult_retraso > 15 THEN 1 ELSE 0 END), 0) AS delayed_over_15_count,
        COALESCE(SUM(CASE WHEN ult_retraso > 60 THEN 1 ELSE 0 END), 0) AS severe_count,
        COALESCE(SUM(CASE WHEN accesible = 1 THEN 1 ELSE 0 END), 0) AS accessible_count,
        COALESCE(SUM(CASE WHEN ult_retraso > 0 THEN ult_retraso ELSE 0 END), 0) AS accumulated_delay_minutes,
        MIN(captured_at) AS min_ts,
        MAX(captured_at) AS max_ts
      FROM train_observations
      WHERE captured_at >= ? AND captured_at <= ?
      `,
      )
      .get(since, until) as
      | {
          observations: number;
          unique_trains: number;
          avg_delay: number;
          max_delay: number;
          on_time_count: number;
          delayed_over_15_count: number;
          severe_count: number;
          accessible_count: number;
          accumulated_delay_minutes: number;
          min_ts: number | null;
          max_ts: number | null;
        }
      | undefined;

    const observationCount = summaryRow?.observations ?? 0;
    const pct = (count: number) =>
      observationCount > 0 ? Number(((count / observationCount) * 100).toFixed(1)) : 0;

    const topProblematicProduct = this.db
      .query(
        `
      SELECT
        cod_product,
        COALESCE(SUM(CASE WHEN ult_retraso > 0 THEN ult_retraso ELSE 0 END), 0) AS accumulated_delay_minutes,
        COALESCE(ROUND(AVG(ult_retraso), 2), 0) AS avg_delay,
        COALESCE(MAX(ult_retraso), 0) AS max_delay,
        COUNT(*) AS observations,
        COUNT(DISTINCT cod_comercial) AS trains
      FROM train_observations
      WHERE captured_at >= ? AND captured_at <= ?
      GROUP BY cod_product
      ORDER BY accumulated_delay_minutes DESC, observations DESC
      LIMIT 1
      `,
      )
      .get(since, until) as
      | {
          cod_product: number;
          accumulated_delay_minutes: number;
          avg_delay: number;
          max_delay: number;
          observations: number;
          trains: number;
        }
      | undefined;

    const topProblematicCorridor = this.db
      .query(
        `
      SELECT
        COALESCE(des_corridor, 'Sin corredor') AS corridor,
        COALESCE(SUM(CASE WHEN ult_retraso > 0 THEN ult_retraso ELSE 0 END), 0) AS accumulated_delay_minutes,
        COALESCE(ROUND(AVG(ult_retraso), 2), 0) AS avg_delay,
        COALESCE(MAX(ult_retraso), 0) AS max_delay,
        COUNT(*) AS observations,
        COUNT(DISTINCT cod_comercial) AS trains
      FROM train_observations
      WHERE captured_at >= ? AND captured_at <= ?
      GROUP BY des_corridor
      ORDER BY accumulated_delay_minutes DESC, observations DESC
      LIMIT 1
      `,
      )
      .get(since, until) as
      | {
          corridor: string;
          accumulated_delay_minutes: number;
          avg_delay: number;
          max_delay: number;
          observations: number;
          trains: number;
        }
      | undefined;

    const byProduct = this.db
      .query(
        `
      SELECT
        cod_product,
        COUNT(*) AS observations,
        COUNT(DISTINCT cod_comercial) AS trains,
        COALESCE(ROUND(AVG(ult_retraso), 2), 0) AS avg_delay,
        COALESCE(MAX(ult_retraso), 0) AS max_delay,
        COALESCE(SUM(CASE WHEN ult_retraso > 0 THEN ult_retraso ELSE 0 END), 0) AS accumulated_delay_minutes
      FROM train_observations
      WHERE captured_at >= ? AND captured_at <= ?
      GROUP BY cod_product
      ORDER BY observations DESC, accumulated_delay_minutes DESC
      LIMIT 10
      `,
      )
      .all(since, until);

    const dailyTrend = this.db
      .query(
        `
      SELECT
        day,
        COALESCE(SUM(observations), 0) AS observations,
        COALESCE(ROUND(SUM(avg_delay * observations) / NULLIF(SUM(observations), 0), 2), 0) AS weighted_avg_delay,
        COALESCE(MAX(max_delay), 0) AS peak_delay,
        COALESCE(ROUND(SUM(total_distance_km), 2), 0) AS km_tracked
      FROM train_daily_stats
      WHERE day >= ? AND day <= ?
      GROUP BY day
      ORDER BY day ASC
      `,
      )
      .all(sinceDay, untilDay);

    const ingestion = this.db
      .query(
        `
      SELECT
        COUNT(*) AS batches,
        COALESCE(ROUND(AVG(train_count), 2), 0) AS avg_trains_per_batch,
        COALESCE(ROUND(MIN(train_count), 2), 0) AS min_trains_per_batch,
        COALESCE(ROUND(MAX(train_count), 2), 0) AS max_trains_per_batch
      FROM ingestion_batches
      WHERE fetched_at >= ? AND fetched_at <= ?
      `,
      )
      .get(since, until) as
      | {
          batches: number;
          avg_trains_per_batch: number;
          min_trains_per_batch: number;
          max_trains_per_batch: number;
        }
      | undefined;

    return {
      hours: safeHours,
      sinceEpoch: since,
      untilEpoch: until,
      nowEpoch: until,
      summary: {
        observations: observationCount,
        uniqueTrains: summaryRow?.unique_trains ?? 0,
        avgDelay: summaryRow?.avg_delay ?? 0,
        maxDelay: summaryRow?.max_delay ?? 0,
        accumulatedDelayMinutes: summaryRow?.accumulated_delay_minutes ?? 0,
        onTimePct: pct(summaryRow?.on_time_count ?? 0),
        delayedOver15Pct: pct(summaryRow?.delayed_over_15_count ?? 0),
        severePct: pct(summaryRow?.severe_count ?? 0),
        accessiblePct: pct(summaryRow?.accessible_count ?? 0),
        minTs: summaryRow?.min_ts ?? null,
        maxTs: summaryRow?.max_ts ?? null,
      },
      topProblematicProduct: topProblematicProduct
        ? {
            codProduct: topProblematicProduct.cod_product,
            accumulatedDelayMinutes: topProblematicProduct.accumulated_delay_minutes,
            avgDelay: topProblematicProduct.avg_delay,
            maxDelay: topProblematicProduct.max_delay,
            observations: topProblematicProduct.observations,
            trains: topProblematicProduct.trains,
          }
        : null,
      topProblematicCorridor: topProblematicCorridor
        ? {
            corridor: topProblematicCorridor.corridor,
            accumulatedDelayMinutes: topProblematicCorridor.accumulated_delay_minutes,
            avgDelay: topProblematicCorridor.avg_delay,
            maxDelay: topProblematicCorridor.max_delay,
            observations: topProblematicCorridor.observations,
            trains: topProblematicCorridor.trains,
          }
        : null,
      byProduct,
      dailyTrend,
      ingestion: {
        batches: ingestion?.batches ?? 0,
        avgTrainsPerBatch: ingestion?.avg_trains_per_batch ?? 0,
        minTrainsPerBatch: ingestion?.min_trains_per_batch ?? 0,
        maxTrainsPerBatch: ingestion?.max_trains_per_batch ?? 0,
      },
    };
  }
}
