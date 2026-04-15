import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { dirname, join } from "node:path";
import {
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
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

interface HistoricalStatsCustomOptions {
  preferAggregated: boolean;
}

interface ArchiveObservationsResult {
  archivedChunks: number;
  archivedRows: number;
  skippedChunks: number;
  deletedRows: number;
}

export class DB {
  private readonly db: Database;
  private readonly dbPath: string;

  constructor(dbPath: string) {
    this.dbPath = dbPath;
    mkdirSync(dirname(dbPath), { recursive: true });
    this.db = new Database(dbPath, { create: true, strict: true });
    this.configure();
    this.createSchema();
    this.ensureSchemaUpgrades();
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
      CREATE UNIQUE INDEX IF NOT EXISTS idx_observations_unique ON train_observations(cod_comercial, captured_at, source);

      CREATE TABLE IF NOT EXISTS observation_archive_chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_ts INTEGER NOT NULL,
        to_ts INTEGER NOT NULL,
        row_count INTEGER NOT NULL,
        file_path TEXT NOT NULL UNIQUE,
        sha256 TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_archive_chunks_range ON observation_archive_chunks(from_ts, to_ts);

      CREATE TABLE IF NOT EXISTS train_hourly_train_stats (
        hour_epoch INTEGER NOT NULL,
        cod_comercial TEXT NOT NULL,
        cod_product INTEGER NOT NULL,
        cod_origen TEXT,
        cod_destino TEXT,
        des_corridor TEXT,
        observations INTEGER NOT NULL DEFAULT 0,
        on_time_count INTEGER NOT NULL DEFAULT 0,
        delayed_over_15_count INTEGER NOT NULL DEFAULT 0,
        severe_count INTEGER NOT NULL DEFAULT 0,
        accessible_count INTEGER NOT NULL DEFAULT 0,
        sum_delay REAL NOT NULL DEFAULT 0,
        sum_positive_delay INTEGER NOT NULL DEFAULT 0,
        max_delay INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (hour_epoch, cod_comercial)
      );
      CREATE INDEX IF NOT EXISTS idx_hourly_time ON train_hourly_train_stats(hour_epoch DESC);
      CREATE INDEX IF NOT EXISTS idx_hourly_product_time ON train_hourly_train_stats(cod_product, hour_epoch DESC);
      CREATE INDEX IF NOT EXISTS idx_hourly_corridor_time ON train_hourly_train_stats(des_corridor, hour_epoch DESC);

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

  private ensureSchemaUpgrades() {
    const alterStatements = [
      `ALTER TABLE train_hourly_train_stats ADD COLUMN cod_origen TEXT`,
      `ALTER TABLE train_hourly_train_stats ADD COLUMN cod_destino TEXT`,
    ];

    for (const statement of alterStatements) {
      try {
        this.db.exec(statement);
      } catch {
        // Ignore if the column already exists or table is unavailable during first bootstrap.
      }
    }

    this.db.exec(
      `CREATE INDEX IF NOT EXISTS idx_hourly_axis_time ON train_hourly_train_stats(cod_origen, cod_destino, hour_epoch DESC);`,
    );

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS observation_archive_chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_ts INTEGER NOT NULL,
        to_ts INTEGER NOT NULL,
        row_count INTEGER NOT NULL,
        file_path TEXT NOT NULL UNIQUE,
        sha256 TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_archive_chunks_range ON observation_archive_chunks(from_ts, to_ts);
    `);

    try {
      this.db.exec(`DROP INDEX IF EXISTS idx_observations_batch;`);
    } catch {
      // Ignore if index was already removed.
    }
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

  upsertHourlyTrainStats(train: NormalizedTrain, capturedAtEpoch: number) {
    const hourEpoch = Math.floor(capturedAtEpoch / 3600) * 3600;
    const delay = train.ultRetraso;

    this.db
      .query(
        `
      INSERT INTO train_hourly_train_stats (
        hour_epoch,
        cod_comercial,
        cod_product,
        cod_origen,
        cod_destino,
        des_corridor,
        observations,
        on_time_count,
        delayed_over_15_count,
        severe_count,
        accessible_count,
        sum_delay,
        sum_positive_delay,
        max_delay
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(hour_epoch, cod_comercial) DO UPDATE SET
        cod_product = excluded.cod_product,
        cod_origen = excluded.cod_origen,
        cod_destino = excluded.cod_destino,
        des_corridor = excluded.des_corridor,
        observations = train_hourly_train_stats.observations + excluded.observations,
        on_time_count = train_hourly_train_stats.on_time_count + excluded.on_time_count,
        delayed_over_15_count =
          train_hourly_train_stats.delayed_over_15_count + excluded.delayed_over_15_count,
        severe_count = train_hourly_train_stats.severe_count + excluded.severe_count,
        accessible_count = train_hourly_train_stats.accessible_count + excluded.accessible_count,
        sum_delay = train_hourly_train_stats.sum_delay + excluded.sum_delay,
        sum_positive_delay = train_hourly_train_stats.sum_positive_delay + excluded.sum_positive_delay,
        max_delay = MAX(train_hourly_train_stats.max_delay, excluded.max_delay)
      `,
      )
      .run(
        hourEpoch,
        train.codComercial,
        train.codProduct,
        train.codOrigen,
        train.codDestino,
        train.desCorridor,
        1,
        delay === 0 ? 1 : 0,
        delay > 15 ? 1 : 0,
        delay > 60 ? 1 : 0,
        train.accesible === 1 ? 1 : 0,
        delay,
        Math.max(0, delay),
        delay,
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

  bootstrapHourlyStatsFromObservations(sinceEpoch: number): number {
    const sinceHour = Math.floor(sinceEpoch / 3600) * 3600;

    const result = this.db
      .query(
        `
      INSERT INTO train_hourly_train_stats (
        hour_epoch,
        cod_comercial,
        cod_product,
        cod_origen,
        cod_destino,
        des_corridor,
        observations,
        on_time_count,
        delayed_over_15_count,
        severe_count,
        accessible_count,
        sum_delay,
        sum_positive_delay,
        max_delay
      )
      SELECT
        CAST((captured_at / 3600) AS INTEGER) * 3600 AS hour_epoch,
        cod_comercial,
        MAX(cod_product) AS cod_product,
        MAX(cod_origen) AS cod_origen,
        MAX(cod_destino) AS cod_destino,
        MAX(des_corridor) AS des_corridor,
        COUNT(*) AS observations,
        SUM(CASE WHEN ult_retraso = 0 THEN 1 ELSE 0 END) AS on_time_count,
        SUM(CASE WHEN ult_retraso > 15 THEN 1 ELSE 0 END) AS delayed_over_15_count,
        SUM(CASE WHEN ult_retraso > 60 THEN 1 ELSE 0 END) AS severe_count,
        SUM(CASE WHEN accesible = 1 THEN 1 ELSE 0 END) AS accessible_count,
        SUM(ult_retraso) AS sum_delay,
        SUM(CASE WHEN ult_retraso > 0 THEN ult_retraso ELSE 0 END) AS sum_positive_delay,
        MAX(ult_retraso) AS max_delay
      FROM train_observations
      WHERE captured_at >= ?
      GROUP BY hour_epoch, cod_comercial
      ON CONFLICT(hour_epoch, cod_comercial) DO UPDATE SET
        cod_product = excluded.cod_product,
        cod_origen = excluded.cod_origen,
        cod_destino = excluded.cod_destino,
        des_corridor = excluded.des_corridor,
        observations = excluded.observations,
        on_time_count = excluded.on_time_count,
        delayed_over_15_count = excluded.delayed_over_15_count,
        severe_count = excluded.severe_count,
        accessible_count = excluded.accessible_count,
        sum_delay = excluded.sum_delay,
        sum_positive_delay = excluded.sum_positive_delay,
        max_delay = excluded.max_delay
      `,
      )
      .run(sinceHour);

    return result.changes;
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

  cleanupHourlyStats(cutoffEpoch: number): number {
    const hourCutoff = Math.floor(cutoffEpoch / 3600) * 3600;
    const result = this.db
      .query(`DELETE FROM train_hourly_train_stats WHERE hour_epoch < ?`)
      .run(hourCutoff);

    return result.changes;
  }

  cleanupDailyStats(cutoffEpoch: number): number {
    const dayCutoff = new Date(cutoffEpoch * 1000).toISOString().slice(0, 10);
    const result = this.db
      .query(`DELETE FROM train_daily_stats WHERE day < ?`)
      .run(dayCutoff);

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

  optimize(walTruncateThresholdBytes = 64 * 1024 * 1024) {
    this.db.exec(`PRAGMA optimize;`);
    const walPath = `${this.dbPath}-wal`;
    let checkpointMode = "PASSIVE";

    try {
      if (walTruncateThresholdBytes > 0 && existsSync(walPath)) {
        const stats = statSync(walPath);
        if (stats.size >= walTruncateThresholdBytes) {
          checkpointMode = "TRUNCATE";
        }
      }
    } catch {
      checkpointMode = "PASSIVE";
    }

    this.db.exec(`PRAGMA wal_checkpoint(${checkpointMode});`);
    return checkpointMode;
  }

  getObservationArchiveStorage() {
    const dbRows = this.db
      .query(`SELECT COUNT(*) AS total FROM train_observations`)
      .get() as { total: number } | undefined;

    const archived = this.db
      .query(
        `
      SELECT
        COALESCE(SUM(row_count), 0) AS archived_rows,
        COUNT(*) AS archive_chunks
      FROM observation_archive_chunks
      `,
      )
      .get() as { archived_rows: number; archive_chunks: number } | undefined;

    return {
      dbRows: dbRows?.total ?? 0,
      archivedRows: archived?.archived_rows ?? 0,
      archiveChunks: archived?.archive_chunks ?? 0,
    };
  }

  archiveObservationsBefore(cutoffEpoch: number, archiveDir: string, zstdLevel: number): ArchiveObservationsResult {
    const hourCutoff = Math.floor(cutoffEpoch / 3600) * 3600;

    const buckets = this.db
      .query(
        `
      SELECT
        CAST((captured_at / 3600) AS INTEGER) * 3600 AS hour_epoch,
        MIN(captured_at) AS from_ts,
        MAX(captured_at) AS to_ts,
        COUNT(*) AS row_count
      FROM train_observations
      WHERE captured_at < ?
      GROUP BY hour_epoch
      ORDER BY hour_epoch ASC
      `,
      )
      .all(hourCutoff) as Array<{
      hour_epoch: number;
      from_ts: number;
      to_ts: number;
      row_count: number;
    }>;

    if (buckets.length === 0) {
      return {
        archivedChunks: 0,
        archivedRows: 0,
        skippedChunks: 0,
        deletedRows: 0,
      };
    }

    const getArchivePathForHour = (hourEpoch: number) => {
      const date = new Date(hourEpoch * 1000);
      const yyyy = String(date.getUTCFullYear());
      const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
      const dd = String(date.getUTCDate()).padStart(2, "0");
      const hh = String(date.getUTCHours()).padStart(2, "0");

      return join(archiveDir, yyyy, mm, dd, `${hh}.jsonl.zst`);
    };

    const archiveLevel = Math.max(1, Math.min(19, Math.trunc(zstdLevel || 3)));
    let archivedChunks = 0;
    let archivedRows = 0;
    let skippedChunks = 0;
    let deletedRows = 0;

    for (const bucket of buckets) {
      const archivePath = getArchivePathForHour(bucket.hour_epoch);
      const existing = this.db
        .query(
          `
        SELECT from_ts, to_ts, row_count
        FROM observation_archive_chunks
        WHERE file_path = ?
        LIMIT 1
        `,
        )
        .get(archivePath) as { from_ts: number; to_ts: number; row_count: number } | undefined;

      if (existing) {
        // Idempotencia: si el manifiesto existe, eliminamos cualquier residuo local de ese rango.
        const deletedResidual = this.db
          .query(`DELETE FROM train_observations WHERE captured_at >= ? AND captured_at <= ?`)
          .run(existing.from_ts, existing.to_ts);
        if (deletedResidual.changes > existing.row_count) {
          throw new Error(
            `residual delete mismatch en ${archivePath}: esperado<=${existing.row_count} actual=${deletedResidual.changes}`,
          );
        }
        if (deletedResidual.changes > 0) {
          deletedRows += deletedResidual.changes;
        }
        skippedChunks += 1;
        continue;
      }

      const rows = this.db
        .query(
          `
        SELECT
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
        FROM train_observations
        WHERE captured_at >= ? AND captured_at <= ?
        ORDER BY captured_at ASC, id ASC
        `,
        )
        .all(bucket.from_ts, bucket.to_ts) as Array<Record<string, unknown>>;

      if (rows.length === 0) {
        continue;
      }

      if (rows.length !== bucket.row_count) {
        throw new Error(
          `row_count mismatch en ${archivePath}: esperado=${bucket.row_count} actual=${rows.length}`,
        );
      }

      const rawContent = rows.map((row) => JSON.stringify(row)).join("\n") + "\n";
      const timestamp = `${Date.now()}-${process.pid}`;
      const tempBasePath = `${archivePath}.tmp-${timestamp}`;
      const rawTempPath = `${tempBasePath}.jsonl`;
      const compressedTempPath = `${tempBasePath}.zst`;

      mkdirSync(dirname(archivePath), { recursive: true });
      writeFileSync(rawTempPath, rawContent, "utf8");

      try {
        const compress = Bun.spawnSync([
          "zstd",
          "-q",
          "-f",
          `-${archiveLevel}`,
          "-o",
          compressedTempPath,
          rawTempPath,
        ]);

        if (compress.exitCode !== 0) {
          const stderr = new TextDecoder().decode(compress.stderr ?? new Uint8Array());
          throw new Error(`zstd fallo para ${archivePath}: ${stderr.trim()}`);
        }

        const fileBuffer = readFileSync(compressedTempPath);
        const sha256 = createHash("sha256").update(fileBuffer).digest("hex");

        const fileFd = openSync(compressedTempPath, "r");
        fsyncSync(fileFd);
        closeSync(fileFd);
        renameSync(compressedTempPath, archivePath);

        const dirFd = openSync(dirname(archivePath), "r");
        fsyncSync(dirFd);
        closeSync(dirFd);

        const archiveCreatedAt = Math.floor(Date.now() / 1000);
        const persist = this.db.transaction(() => {
          this.db
            .query(
              `
            INSERT INTO observation_archive_chunks (
              from_ts,
              to_ts,
              row_count,
              file_path,
              sha256,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            `,
            )
            .run(bucket.from_ts, bucket.to_ts, rows.length, archivePath, sha256, archiveCreatedAt);

          const deleted = this.db
            .query(`DELETE FROM train_observations WHERE captured_at >= ? AND captured_at <= ?`)
            .run(bucket.from_ts, bucket.to_ts);

          if (deleted.changes !== rows.length) {
            throw new Error(
              `delete mismatch en ${archivePath}: esperado=${rows.length} actual=${deleted.changes}`,
            );
          }
        });
        persist();

        archivedChunks += 1;
        archivedRows += rows.length;
        deletedRows += rows.length;
      } finally {
        try {
          unlinkSync(rawTempPath);
        } catch {
          // ignore temp cleanup errors
        }

        try {
          unlinkSync(compressedTempPath);
        } catch {
          // ignore temp cleanup errors
        }
      }
    }

    return {
      archivedChunks,
      archivedRows,
      skippedChunks,
      deletedRows,
    };
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

  getCorridorMeta() {
    const now = Math.floor(Date.now() / 1000);
    const since30d = now - 30 * 24 * 3600;

    const availability = this.db
      .query(
        `
      SELECT
        COUNT(*) AS total,
        COALESCE(
          SUM(
            CASE
              WHEN des_corridor IS NOT NULL AND TRIM(des_corridor) <> '' THEN 1
              ELSE 0
            END
          ),
          0
        ) AS with_official
      FROM train_observations
      WHERE captured_at >= ?
      `,
      )
      .get(since30d) as { total: number; with_official: number } | undefined;

    const cutRow = this.db
      .query(
        `
      WITH last_non_empty AS (
        SELECT MAX(captured_at) AS ts
        FROM train_observations
        WHERE des_corridor IS NOT NULL AND TRIM(des_corridor) <> ''
      )
      SELECT MIN(captured_at) AS detected_missing_since
      FROM train_observations
      WHERE (des_corridor IS NULL OR TRIM(des_corridor) = '')
        AND captured_at > COALESCE((SELECT ts FROM last_non_empty), -1)
      `,
      )
      .get() as { detected_missing_since: number | null } | undefined;

    const total = availability?.total ?? 0;
    const withOfficial = availability?.with_official ?? 0;

    return {
      mode: "official_or_derived_axis",
      officialAvailablePct: total > 0 ? Number(((withOfficial / total) * 100).toFixed(1)) : 0,
      detectedMissingSince: cutRow?.detected_missing_since ?? null,
      detectedMissingSinceIso: cutRow?.detected_missing_since
        ? new Date(cutRow.detected_missing_since * 1000).toISOString()
        : null,
      samplesWindowHours: 24 * 30,
    };
  }

  getTopCorridors(limit: number) {
    return this.db
      .query(
        `
      WITH base AS (
        SELECT
          t.cod_comercial,
          t.cod_origen,
          t.cod_destino,
          t.des_corridor,
          t.ult_retraso,
          so.name AS origin_name,
          sd.name AS destination_name
        FROM trains_current t
        LEFT JOIN stations so ON so.code = t.cod_origen
        LEFT JOIN stations sd ON sd.code = t.cod_destino
      ),
      normalized AS (
        SELECT
          cod_comercial,
          ult_retraso,
          CASE
            WHEN des_corridor IS NOT NULL AND TRIM(des_corridor) <> '' THEN 'official'
            ELSE 'derived_axis'
          END AS source,
          CASE
            WHEN des_corridor IS NOT NULL AND TRIM(des_corridor) <> '' THEN 'official:' || LOWER(TRIM(des_corridor))
            ELSE
              'axis:' ||
              CASE
                WHEN COALESCE(cod_origen, '') <= COALESCE(cod_destino, '')
                  THEN COALESCE(cod_origen, '~') || '|' || COALESCE(cod_destino, '~')
                ELSE COALESCE(cod_destino, '~') || '|' || COALESCE(cod_origen, '~')
              END
          END AS axis_key,
          CASE
            WHEN des_corridor IS NOT NULL AND TRIM(des_corridor) <> '' THEN TRIM(des_corridor)
            ELSE
              CASE
                WHEN COALESCE(cod_origen, '') <= COALESCE(cod_destino, '')
                  THEN COALESCE(origin_name, cod_origen, 'Origen desconocido') || ' ↔ ' || COALESCE(destination_name, cod_destino, 'Destino desconocido')
                ELSE COALESCE(destination_name, cod_destino, 'Destino desconocido') || ' ↔ ' || COALESCE(origin_name, cod_origen, 'Origen desconocido')
              END
          END AS axis_label
        FROM base
      )
      SELECT
        axis_key AS axisKey,
        axis_label AS axisLabel,
        axis_label AS corridor,
        source,
        COUNT(*) AS train_count,
        COALESCE(ROUND(AVG(ult_retraso), 2), 0) AS avg_delay,
        COALESCE(MAX(ult_retraso), 0) AS max_delay
      FROM normalized
      GROUP BY source, axis_key, axis_label
      ORDER BY train_count DESC, avg_delay DESC
      LIMIT ?
      `,
      )
      .all(limit);
  }

  private getAccountabilityThresholds(hours: number) {
    if (hours <= 24) {
      return { minRouteObservations: 40, minTrainObservations: 20 };
    }

    if (hours <= 168) {
      return { minRouteObservations: 200, minTrainObservations: 80 };
    }

    return { minRouteObservations: 600, minTrainObservations: 240 };
  }

  private getWorseningVs7d(until: number) {
    const since24 = until - 24 * 3600;
    const since7d = until - 7 * 24 * 3600;

    const row = this.db
      .query(
        `
      SELECT
        COALESCE(SUM(CASE WHEN captured_at >= ? THEN 1 ELSE 0 END), 0) AS obs_24h,
        COALESCE(SUM(CASE WHEN captured_at >= ? THEN 1 ELSE 0 END), 0) AS obs_7d,
        COALESCE(SUM(CASE WHEN captured_at >= ? AND ult_retraso > 15 THEN 1 ELSE 0 END), 0) AS delayed_24h,
        COALESCE(SUM(CASE WHEN captured_at >= ? AND ult_retraso > 15 THEN 1 ELSE 0 END), 0) AS delayed_7d,
        COALESCE(SUM(CASE WHEN captured_at >= ? AND ult_retraso > 60 THEN 1 ELSE 0 END), 0) AS severe_24h,
        COALESCE(SUM(CASE WHEN captured_at >= ? AND ult_retraso > 60 THEN 1 ELSE 0 END), 0) AS severe_7d
      FROM train_observations
      WHERE captured_at >= ? AND captured_at <= ?
      `,
      )
      .get(since24, since7d, since24, since7d, since24, since7d, since7d, until) as
      | {
          obs_24h: number;
          obs_7d: number;
          delayed_24h: number;
          delayed_7d: number;
          severe_24h: number;
          severe_7d: number;
        }
      | undefined;

    const obs24 = row?.obs_24h ?? 0;
    const obs7d = row?.obs_7d ?? 0;
    const delayed24Pct = obs24 > 0 ? Number((((row?.delayed_24h ?? 0) / obs24) * 100).toFixed(1)) : 0;
    const delayed7Pct = obs7d > 0 ? Number((((row?.delayed_7d ?? 0) / obs7d) * 100).toFixed(1)) : 0;
    const severe24Pct = obs24 > 0 ? Number((((row?.severe_24h ?? 0) / obs24) * 100).toFixed(1)) : 0;
    const severe7Pct = obs7d > 0 ? Number((((row?.severe_7d ?? 0) / obs7d) * 100).toFixed(1)) : 0;

    const deltaDelayed15Pct = Number((delayed24Pct - delayed7Pct).toFixed(1));
    const deltaSevere60Pct = Number((severe24Pct - severe7Pct).toFixed(1));

    let trend = "flat";
    if (deltaDelayed15Pct > 0.5 || deltaSevere60Pct > 0.3) {
      trend = "worsening";
    } else if (deltaDelayed15Pct < -0.5 || deltaSevere60Pct < -0.3) {
      trend = "improving";
    }

    return {
      trend,
      current24hDelayed15Pct: delayed24Pct,
      baseline7dDelayed15Pct: delayed7Pct,
      deltaDelayed15Pct,
      current24hSevere60Pct: severe24Pct,
      baseline7dSevere60Pct: severe7Pct,
      deltaSevere60Pct,
    };
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
        CAST(COALESCE(SUM(sum_positive_delay), 0) AS TEXT) AS accumulated_delay_minutes,
        COALESCE(SUM(observations), 0) AS observations,
        COUNT(DISTINCT cod_comercial) AS affected_trains
      FROM train_hourly_train_stats
      WHERE hour_epoch >= ?
      GROUP BY cod_product
      ORDER BY COALESCE(SUM(sum_positive_delay), 0) DESC, observations DESC
      LIMIT 1
      `,
      )
      .get(dayStartEpoch) as
      | {
          cod_product: number;
          accumulated_delay_minutes: string;
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

  private getCorridorAggregateRows(rangeRowsCte: string, rangeParams: number[]) {
    return this.db
      .query(
        `
      ${rangeRowsCte},
      corridor_rows AS (
        SELECT
          rr.cod_comercial,
          rr.observations,
          rr.sum_delay,
          rr.sum_positive_delay,
          rr.max_delay,
          rr.delayed_over_15_count,
          rr.severe_count,
          CASE
            WHEN rr.des_corridor IS NOT NULL AND TRIM(rr.des_corridor) <> '' THEN 'official'
            ELSE 'derived_axis'
          END AS source,
          CASE
            WHEN rr.des_corridor IS NOT NULL AND TRIM(rr.des_corridor) <> '' THEN 'official:' || LOWER(TRIM(rr.des_corridor))
            ELSE
              'axis:' ||
              CASE
                WHEN COALESCE(rr.cod_origen, '') <= COALESCE(rr.cod_destino, '')
                  THEN COALESCE(rr.cod_origen, '~') || '|' || COALESCE(rr.cod_destino, '~')
                ELSE COALESCE(rr.cod_destino, '~') || '|' || COALESCE(rr.cod_origen, '~')
              END
          END AS axis_key,
          CASE
            WHEN rr.des_corridor IS NOT NULL AND TRIM(rr.des_corridor) <> '' THEN TRIM(rr.des_corridor)
            ELSE
              CASE
                WHEN COALESCE(rr.cod_origen, '') <= COALESCE(rr.cod_destino, '')
                  THEN COALESCE(so.name, rr.cod_origen, 'Origen desconocido') || ' ↔ ' || COALESCE(sd.name, rr.cod_destino, 'Destino desconocido')
                ELSE COALESCE(sd.name, rr.cod_destino, 'Destino desconocido') || ' ↔ ' || COALESCE(so.name, rr.cod_origen, 'Origen desconocido')
              END
          END AS axis_label
        FROM range_rows rr
        LEFT JOIN stations so ON so.code = rr.cod_origen
        LEFT JOIN stations sd ON sd.code = rr.cod_destino
      ),
      corridor_agg AS (
        SELECT
          source,
          axis_key,
          axis_label,
          COALESCE(SUM(observations), 0) AS observations,
          COUNT(DISTINCT cod_comercial) AS trains,
          COALESCE(SUM(sum_positive_delay), 0) AS accumulated_delay_minutes_int,
          CAST(COALESCE(SUM(sum_positive_delay), 0) AS TEXT) AS accumulated_delay_minutes,
          COALESCE(ROUND(SUM(sum_delay) / NULLIF(SUM(observations), 0), 2), 0) AS avg_delay,
          COALESCE(MAX(max_delay), 0) AS max_delay,
          COALESCE(ROUND(100.0 * SUM(delayed_over_15_count) / NULLIF(SUM(observations), 0), 1), 0) AS delayed_over_15_pct,
          COALESCE(ROUND(100.0 * SUM(severe_count) / NULLIF(SUM(observations), 0), 1), 0) AS severe_pct
        FROM corridor_rows
        GROUP BY source, axis_key, axis_label
      )
      SELECT
        source,
        axis_key,
        axis_label,
        observations,
        trains,
        accumulated_delay_minutes_int,
        accumulated_delay_minutes,
        avg_delay,
        max_delay,
        delayed_over_15_pct,
        severe_pct
      FROM corridor_agg
      `,
      )
      .all(...rangeParams) as Array<{
      source: string;
      axis_key: string;
      axis_label: string;
      observations: number;
      trains: number;
      accumulated_delay_minutes_int: number;
      accumulated_delay_minutes: string;
      avg_delay: number;
      max_delay: number;
      delayed_over_15_pct: number;
      severe_pct: number;
    }>;
  }

  private getRepeatOffenders(rangeRowsCte: string, rangeParams: number[], minObservations: number) {
    return this.db
      .query(
        `
      ${rangeRowsCte},
      offender_agg AS (
        SELECT
          rr.cod_comercial,
          MAX(rr.cod_product) AS cod_product,
          MAX(rr.cod_origen) AS cod_origen,
          MAX(rr.cod_destino) AS cod_destino,
          COALESCE(SUM(rr.observations), 0) AS observations,
          COALESCE(ROUND(SUM(rr.sum_delay) / NULLIF(SUM(rr.observations), 0), 2), 0) AS avg_delay,
          COALESCE(MAX(rr.max_delay), 0) AS max_delay,
          COALESCE(ROUND(100.0 * SUM(rr.delayed_over_15_count) / NULLIF(SUM(rr.observations), 0), 1), 0) AS delayed_over_15_pct,
          COALESCE(ROUND(100.0 * SUM(rr.severe_count) / NULLIF(SUM(rr.observations), 0), 1), 0) AS severe_pct
        FROM range_rows rr
        GROUP BY rr.cod_comercial
        HAVING SUM(rr.observations) >= ?
      )
      SELECT
        oa.cod_comercial,
        oa.cod_product,
        oa.observations,
        oa.avg_delay,
        oa.max_delay,
        oa.delayed_over_15_pct,
        oa.severe_pct,
        COALESCE(so.name, oa.cod_origen, 'Origen desconocido') AS origin,
        COALESCE(sd.name, oa.cod_destino, 'Destino desconocido') AS destination
      FROM offender_agg oa
      LEFT JOIN stations so ON so.code = oa.cod_origen
      LEFT JOIN stations sd ON sd.code = oa.cod_destino
      ORDER BY oa.delayed_over_15_pct DESC, oa.severe_pct DESC, oa.observations DESC
      LIMIT 12
      `,
      )
      .all(...rangeParams, minObservations) as Array<{
      cod_comercial: string;
      cod_product: number;
      observations: number;
      avg_delay: number;
      max_delay: number;
      delayed_over_15_pct: number;
      severe_pct: number;
      origin: string;
      destination: string;
    }>;
  }

  getHistoricalStats(hours: number) {
    const now = Math.floor(Date.now() / 1000);
    const safeHours = Math.max(24, Math.min(24 * 30, Math.trunc(hours)));
    const since = now - safeHours * 3600;

    const hourlyBootstrapReady = this.getState("hourly_bootstrap_last_30d_v2");
    if (!hourlyBootstrapReady) {
      return this.getHistoricalStatsByObservationRange(since, now);
    }

    return this.getHistoricalStatsByHourlyRange(since, now);
  }

  getHistoricalStatsCustom(sinceEpoch: number, untilEpoch: number, options?: HistoricalStatsCustomOptions) {
    const now = Math.floor(Date.now() / 1000);
    const safeSince = Math.max(0, Math.min(sinceEpoch, untilEpoch));
    const safeUntil = Math.max(safeSince + 1, Math.min(untilEpoch, now));

    if (options?.preferAggregated) {
      return this.getHistoricalStatsByHourlyRange(safeSince, safeUntil);
    }

    return this.getHistoricalStatsByObservationRange(safeSince, safeUntil);
  }

  private getHistoricalStatsByHourlyRange(since: number, until: number) {
    const safeHours = Math.max(1, Math.round((until - since) / 3600));
    const startHour = Math.floor(since / 3600) * 3600;
    const endHour = Math.floor(until / 3600) * 3600;
    const fullFromHour = startHour + 3600;
    const fullToHour = endHour - 3600;
    const firstEdgeFrom = since;
    const firstEdgeTo = Math.min(until, startHour + 3599);
    const hasSecondEdge = endHour !== startHour && until >= endHour;
    const secondEdgeFrom = hasSecondEdge ? endHour : -1;
    const secondEdgeTo = hasSecondEdge ? until : -2;
    const rangeParams = [
      fullFromHour,
      fullToHour,
      firstEdgeFrom,
      firstEdgeTo,
      secondEdgeFrom,
      secondEdgeTo,
    ];
    const sinceDay = new Date(since * 1000).toISOString().slice(0, 10);
    const untilDay = new Date(until * 1000).toISOString().slice(0, 10);

    const rangeRowsCte = `
      WITH range_rows AS (
        SELECT
          hour_epoch,
          cod_comercial,
          cod_product,
          cod_origen,
          cod_destino,
          des_corridor,
          observations,
          on_time_count,
          delayed_over_15_count,
          severe_count,
          accessible_count,
          sum_delay,
          sum_positive_delay,
          max_delay
        FROM train_hourly_train_stats
        WHERE hour_epoch >= ? AND hour_epoch <= ?

        UNION ALL

        SELECT
          CAST((captured_at / 3600) AS INTEGER) * 3600 AS hour_epoch,
          cod_comercial,
          MAX(cod_product) AS cod_product,
          MAX(cod_origen) AS cod_origen,
          MAX(cod_destino) AS cod_destino,
          MAX(des_corridor) AS des_corridor,
          COUNT(*) AS observations,
          SUM(CASE WHEN ult_retraso = 0 THEN 1 ELSE 0 END) AS on_time_count,
          SUM(CASE WHEN ult_retraso > 15 THEN 1 ELSE 0 END) AS delayed_over_15_count,
          SUM(CASE WHEN ult_retraso > 60 THEN 1 ELSE 0 END) AS severe_count,
          SUM(CASE WHEN accesible = 1 THEN 1 ELSE 0 END) AS accessible_count,
          SUM(ult_retraso) AS sum_delay,
          SUM(CASE WHEN ult_retraso > 0 THEN ult_retraso ELSE 0 END) AS sum_positive_delay,
          MAX(ult_retraso) AS max_delay
        FROM train_observations
        WHERE (captured_at >= ? AND captured_at <= ?)
          OR (captured_at >= ? AND captured_at <= ?)
        GROUP BY hour_epoch, cod_comercial
      )
    `;

    const summaryRow = this.db
      .query(
        `
      ${rangeRowsCte}
      SELECT
        COALESCE(SUM(observations), 0) AS observations,
        COUNT(DISTINCT cod_comercial) AS unique_trains,
        COALESCE(ROUND(SUM(sum_delay) / NULLIF(SUM(observations), 0), 2), 0) AS avg_delay,
        COALESCE(MAX(max_delay), 0) AS max_delay,
        COALESCE(SUM(on_time_count), 0) AS on_time_count,
        COALESCE(SUM(delayed_over_15_count), 0) AS delayed_over_15_count,
        COALESCE(SUM(severe_count), 0) AS severe_count,
        COALESCE(SUM(accessible_count), 0) AS accessible_count,
        CAST(COALESCE(SUM(sum_positive_delay), 0) AS TEXT) AS accumulated_delay_minutes,
        MIN(hour_epoch) AS min_ts,
        MAX(hour_epoch) AS max_ts
      FROM range_rows
      `,
      )
      .get(...rangeParams) as
      | {
          observations: number;
          unique_trains: number;
          avg_delay: number;
          max_delay: number;
          on_time_count: number;
          delayed_over_15_count: number;
          severe_count: number;
          accessible_count: number;
          accumulated_delay_minutes: string;
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
      ${rangeRowsCte}
      SELECT
        cod_product,
        CAST(COALESCE(SUM(sum_positive_delay), 0) AS TEXT) AS accumulated_delay_minutes,
        COALESCE(ROUND(SUM(sum_delay) / NULLIF(SUM(observations), 0), 2), 0) AS avg_delay,
        COALESCE(MAX(max_delay), 0) AS max_delay,
        COALESCE(SUM(observations), 0) AS observations,
        COUNT(DISTINCT cod_comercial) AS trains
      FROM range_rows
      GROUP BY cod_product
      ORDER BY COALESCE(SUM(sum_positive_delay), 0) DESC, observations DESC
      LIMIT 1
      `,
      )
      .get(...rangeParams) as
      | {
          cod_product: number;
          accumulated_delay_minutes: string;
          avg_delay: number;
          max_delay: number;
          observations: number;
          trains: number;
        }
      | undefined;

    const corridorAggregates = this.getCorridorAggregateRows(rangeRowsCte, rangeParams);
    const topProblematicCorridor = corridorAggregates
      .slice()
      .sort(
        (a, b) =>
          b.accumulated_delay_minutes_int - a.accumulated_delay_minutes_int ||
          b.observations - a.observations,
      )[0];

    const thresholds = this.getAccountabilityThresholds(safeHours);
    const criticalRoutes = corridorAggregates
      .filter((row) => row.observations >= thresholds.minRouteObservations)
      .sort(
        (a, b) =>
          b.delayed_over_15_pct - a.delayed_over_15_pct ||
          b.severe_pct - a.severe_pct ||
          b.observations - a.observations,
      )
      .slice(0, 12)
      .map((row) => ({
        axisKey: row.axis_key,
        axisLabel: row.axis_label,
        source: row.source,
        observations: row.observations,
        avgDelay: row.avg_delay,
        maxDelay: row.max_delay,
        delayed15Pct: row.delayed_over_15_pct,
        severe60Pct: row.severe_pct,
      }));

    const repeatOffenders = this.getRepeatOffenders(
      rangeRowsCte,
      rangeParams,
      thresholds.minTrainObservations,
    ).map((row) => ({
      codComercial: row.cod_comercial,
      codProduct: row.cod_product,
      observations: row.observations,
      avgDelay: row.avg_delay,
      maxDelay: row.max_delay,
      delayed15Pct: row.delayed_over_15_pct,
      severe60Pct: row.severe_pct,
      origin: row.origin,
      destination: row.destination,
    }));

    const worseningVs7d = this.getWorseningVs7d(until);

    const byProduct = this.db
      .query(
        `
      ${rangeRowsCte}
      SELECT
        cod_product,
        COALESCE(SUM(observations), 0) AS observations,
        COUNT(DISTINCT cod_comercial) AS trains,
        COALESCE(ROUND(SUM(sum_delay) / NULLIF(SUM(observations), 0), 2), 0) AS avg_delay,
        COALESCE(MAX(max_delay), 0) AS max_delay,
        CAST(COALESCE(SUM(sum_positive_delay), 0) AS TEXT) AS accumulated_delay_minutes
      FROM range_rows
      GROUP BY cod_product
      ORDER BY observations DESC, COALESCE(SUM(sum_positive_delay), 0) DESC
      LIMIT 10
      `,
      )
      .all(...rangeParams);

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
        accumulatedDelayMinutes: summaryRow?.accumulated_delay_minutes ?? "0",
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
            corridor: topProblematicCorridor.axis_label,
            axisKey: topProblematicCorridor.axis_key,
            axisLabel: topProblematicCorridor.axis_label,
            source: topProblematicCorridor.source,
            accumulatedDelayMinutes: topProblematicCorridor.accumulated_delay_minutes,
            avgDelay: topProblematicCorridor.avg_delay,
            maxDelay: topProblematicCorridor.max_delay,
            observations: topProblematicCorridor.observations,
            trains: topProblematicCorridor.trains,
          }
        : null,
      accountability: {
        summary: {
          delayed15Pct: pct(summaryRow?.delayed_over_15_count ?? 0),
          severe60Pct: pct(summaryRow?.severe_count ?? 0),
          observations: observationCount,
          uniqueTrains: summaryRow?.unique_trains ?? 0,
          worseningVs7d,
        },
        criticalRoutes,
        repeatOffenders,
      },
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

  private getHistoricalStatsByObservationRange(since: number, until: number) {
    const safeHours = Math.max(1, Math.round((until - since) / 3600));
    const sinceDay = new Date(since * 1000).toISOString().slice(0, 10);
    const untilDay = new Date(until * 1000).toISOString().slice(0, 10);
    const rangeParams = [since, until];

    const rangeRowsCte = `
      WITH range_rows AS (
        SELECT
          CAST((captured_at / 3600) AS INTEGER) * 3600 AS hour_epoch,
          cod_comercial,
          MAX(cod_product) AS cod_product,
          MAX(cod_origen) AS cod_origen,
          MAX(cod_destino) AS cod_destino,
          MAX(des_corridor) AS des_corridor,
          COUNT(*) AS observations,
          SUM(CASE WHEN ult_retraso = 0 THEN 1 ELSE 0 END) AS on_time_count,
          SUM(CASE WHEN ult_retraso > 15 THEN 1 ELSE 0 END) AS delayed_over_15_count,
          SUM(CASE WHEN ult_retraso > 60 THEN 1 ELSE 0 END) AS severe_count,
          SUM(CASE WHEN accesible = 1 THEN 1 ELSE 0 END) AS accessible_count,
          SUM(ult_retraso) AS sum_delay,
          SUM(CASE WHEN ult_retraso > 0 THEN ult_retraso ELSE 0 END) AS sum_positive_delay,
          MAX(ult_retraso) AS max_delay
        FROM train_observations
        WHERE captured_at >= ? AND captured_at <= ?
        GROUP BY hour_epoch, cod_comercial
      )
    `;

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
        CAST(COALESCE(SUM(CASE WHEN ult_retraso > 0 THEN ult_retraso ELSE 0 END), 0) AS TEXT) AS accumulated_delay_minutes,
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
          accumulated_delay_minutes: string;
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
        CAST(COALESCE(SUM(CASE WHEN ult_retraso > 0 THEN ult_retraso ELSE 0 END), 0) AS TEXT) AS accumulated_delay_minutes,
        COALESCE(ROUND(AVG(ult_retraso), 2), 0) AS avg_delay,
        COALESCE(MAX(ult_retraso), 0) AS max_delay,
        COUNT(*) AS observations,
        COUNT(DISTINCT cod_comercial) AS trains
      FROM train_observations
      WHERE captured_at >= ? AND captured_at <= ?
      GROUP BY cod_product
      ORDER BY COALESCE(SUM(CASE WHEN ult_retraso > 0 THEN ult_retraso ELSE 0 END), 0) DESC, observations DESC
      LIMIT 1
      `,
      )
      .get(since, until) as
      | {
          cod_product: number;
          accumulated_delay_minutes: string;
          avg_delay: number;
          max_delay: number;
          observations: number;
          trains: number;
        }
      | undefined;

    const corridorAggregates = this.getCorridorAggregateRows(rangeRowsCte, rangeParams);
    const topProblematicCorridor = corridorAggregates
      .slice()
      .sort(
        (a, b) =>
          b.accumulated_delay_minutes_int - a.accumulated_delay_minutes_int ||
          b.observations - a.observations,
      )[0];

    const thresholds = this.getAccountabilityThresholds(safeHours);
    const criticalRoutes = corridorAggregates
      .filter((row) => row.observations >= thresholds.minRouteObservations)
      .sort(
        (a, b) =>
          b.delayed_over_15_pct - a.delayed_over_15_pct ||
          b.severe_pct - a.severe_pct ||
          b.observations - a.observations,
      )
      .slice(0, 12)
      .map((row) => ({
        axisKey: row.axis_key,
        axisLabel: row.axis_label,
        source: row.source,
        observations: row.observations,
        avgDelay: row.avg_delay,
        maxDelay: row.max_delay,
        delayed15Pct: row.delayed_over_15_pct,
        severe60Pct: row.severe_pct,
      }));

    const repeatOffenders = this.getRepeatOffenders(
      rangeRowsCte,
      rangeParams,
      thresholds.minTrainObservations,
    ).map((row) => ({
      codComercial: row.cod_comercial,
      codProduct: row.cod_product,
      observations: row.observations,
      avgDelay: row.avg_delay,
      maxDelay: row.max_delay,
      delayed15Pct: row.delayed_over_15_pct,
      severe60Pct: row.severe_pct,
      origin: row.origin,
      destination: row.destination,
    }));

    const worseningVs7d = this.getWorseningVs7d(until);

    const byProduct = this.db
      .query(
        `
      SELECT
        cod_product,
        COUNT(*) AS observations,
        COUNT(DISTINCT cod_comercial) AS trains,
        COALESCE(ROUND(AVG(ult_retraso), 2), 0) AS avg_delay,
        COALESCE(MAX(ult_retraso), 0) AS max_delay,
        CAST(COALESCE(SUM(CASE WHEN ult_retraso > 0 THEN ult_retraso ELSE 0 END), 0) AS TEXT) AS accumulated_delay_minutes
      FROM train_observations
      WHERE captured_at >= ? AND captured_at <= ?
      GROUP BY cod_product
      ORDER BY observations DESC, COALESCE(SUM(CASE WHEN ult_retraso > 0 THEN ult_retraso ELSE 0 END), 0) DESC
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
        accumulatedDelayMinutes: summaryRow?.accumulated_delay_minutes ?? "0",
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
            corridor: topProblematicCorridor.axis_label,
            axisKey: topProblematicCorridor.axis_key,
            axisLabel: topProblematicCorridor.axis_label,
            source: topProblematicCorridor.source,
            accumulatedDelayMinutes: topProblematicCorridor.accumulated_delay_minutes,
            avgDelay: topProblematicCorridor.avg_delay,
            maxDelay: topProblematicCorridor.max_delay,
            observations: topProblematicCorridor.observations,
            trains: topProblematicCorridor.trains,
          }
        : null,
      accountability: {
        summary: {
          delayed15Pct: pct(summaryRow?.delayed_over_15_count ?? 0),
          severe60Pct: pct(summaryRow?.severe_count ?? 0),
          observations: observationCount,
          uniqueTrains: summaryRow?.unique_trains ?? 0,
          worseningVs7d,
        },
        criticalRoutes,
        repeatOffenders,
      },
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
