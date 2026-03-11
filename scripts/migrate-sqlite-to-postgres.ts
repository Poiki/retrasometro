import { existsSync } from "node:fs";
import { Database } from "bun:sqlite";
import { Client } from "pg";

type SqlRow = Record<string, unknown>;

const sqlitePath = Bun.env.SQLITE_PATH ?? "./data/renfe.db";
const postgresUrl = Bun.env.POSTGRES_URL ?? Bun.env.DATABASE_URL;
const batchSize = Number(Bun.env.MIGRATION_BATCH_SIZE ?? 500);

if (!postgresUrl) {
  throw new Error("Define POSTGRES_URL o DATABASE_URL para ejecutar la migracion.");
}

if (!existsSync(sqlitePath)) {
  throw new Error(`No existe la base sqlite en ${sqlitePath}`);
}

const sqlite = new Database(sqlitePath, { readonly: true });
const pg = new Client({ connectionString: postgresUrl });

const nowEpoch = () => Math.floor(Date.now() / 1000);

const createSchema = async (client: Client) => {
  await client.query(`
    CREATE TABLE IF NOT EXISTS app_state (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at BIGINT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS ingestion_runs (
      id BIGSERIAL PRIMARY KEY,
      fetched_at BIGINT NOT NULL,
      source TEXT NOT NULL,
      success INTEGER NOT NULL,
      train_count INTEGER NOT NULL DEFAULT 0,
      skipped INTEGER NOT NULL DEFAULT 0,
      error TEXT,
      provider_updated_at TEXT
    );

    CREATE TABLE IF NOT EXISTS ingestion_batches (
      id BIGSERIAL PRIMARY KEY,
      fetched_at BIGINT NOT NULL,
      source TEXT NOT NULL,
      provider_updated_at TEXT,
      train_count INTEGER NOT NULL DEFAULT 0,
      payload_hash TEXT,
      created_at BIGINT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS stations (
      code TEXT PRIMARY KEY,
      name TEXT,
      locality TEXT,
      province TEXT,
      accessible INTEGER,
      attended INTEGER,
      correspondences TEXT,
      level TEXT,
      lat DOUBLE PRECISION,
      lon DOUBLE PRECISION,
      updated_at BIGINT NOT NULL
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
      latitud DOUBLE PRECISION NOT NULL,
      longitud DOUBLE PRECISION NOT NULL,
      gps_time BIGINT,
      p TEXT,
      mat TEXT,
      first_seen_at BIGINT NOT NULL,
      last_seen_at BIGINT NOT NULL,
      last_payload_hash TEXT,
      last_snapshot_at BIGINT
    );

    CREATE TABLE IF NOT EXISTS train_snapshots (
      id BIGSERIAL PRIMARY KEY,
      cod_comercial TEXT NOT NULL,
      captured_at BIGINT NOT NULL,
      cod_product INTEGER NOT NULL,
      cod_origen TEXT,
      cod_destino TEXT,
      cod_est_ant TEXT,
      cod_est_sig TEXT,
      hora_llegada_sig_est TEXT,
      des_corridor TEXT,
      accesible INTEGER NOT NULL,
      ult_retraso INTEGER NOT NULL,
      latitud DOUBLE PRECISION NOT NULL,
      longitud DOUBLE PRECISION NOT NULL,
      gps_time BIGINT,
      p TEXT,
      mat TEXT,
      hash TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS train_observations (
      id BIGSERIAL PRIMARY KEY,
      batch_id BIGINT,
      cod_comercial TEXT NOT NULL,
      captured_at BIGINT NOT NULL,
      cod_product INTEGER NOT NULL,
      cod_origen TEXT,
      cod_destino TEXT,
      cod_est_ant TEXT,
      cod_est_sig TEXT,
      hora_llegada_sig_est TEXT,
      des_corridor TEXT,
      accesible INTEGER NOT NULL,
      ult_retraso INTEGER NOT NULL,
      latitud DOUBLE PRECISION NOT NULL,
      longitud DOUBLE PRECISION NOT NULL,
      gps_time BIGINT,
      p TEXT,
      mat TEXT,
      hash TEXT NOT NULL,
      source TEXT NOT NULL DEFAULT 'live',
      is_estimated INTEGER NOT NULL DEFAULT 0,
      CONSTRAINT fk_observations_batch FOREIGN KEY(batch_id) REFERENCES ingestion_batches(id) ON DELETE SET NULL,
      CONSTRAINT uq_observations UNIQUE(cod_comercial, captured_at, source)
    );

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
      avg_delay DOUBLE PRECISION NOT NULL DEFAULT 0,
      max_delay INTEGER NOT NULL DEFAULT 0,
      min_delay INTEGER NOT NULL DEFAULT 0,
      total_distance_km DOUBLE PRECISION NOT NULL DEFAULT 0,
      first_seen_at BIGINT NOT NULL,
      last_seen_at BIGINT NOT NULL,
      PRIMARY KEY(day, cod_comercial)
    );

    CREATE INDEX IF NOT EXISTS idx_ingestion_fetched_at ON ingestion_runs(fetched_at DESC);
    CREATE INDEX IF NOT EXISTS idx_batches_fetched_at ON ingestion_batches(fetched_at DESC);
    CREATE INDEX IF NOT EXISTS idx_current_product ON trains_current(cod_product);
    CREATE INDEX IF NOT EXISTS idx_current_last_seen ON trains_current(last_seen_at DESC);
    CREATE INDEX IF NOT EXISTS idx_current_delay ON trains_current(ult_retraso DESC);
    CREATE INDEX IF NOT EXISTS idx_snapshots_train_time ON train_snapshots(cod_comercial, captured_at DESC);
    CREATE INDEX IF NOT EXISTS idx_snapshots_time ON train_snapshots(captured_at DESC);
    CREATE INDEX IF NOT EXISTS idx_observations_train_time ON train_observations(cod_comercial, captured_at DESC);
    CREATE INDEX IF NOT EXISTS idx_observations_time ON train_observations(captured_at DESC);
    CREATE INDEX IF NOT EXISTS idx_observations_batch ON train_observations(batch_id);
    CREATE INDEX IF NOT EXISTS idx_daily_day ON train_daily_stats(day DESC);
    CREATE INDEX IF NOT EXISTS idx_daily_product_day ON train_daily_stats(cod_product, day DESC);
  `);
};

const truncateTarget = async (client: Client) => {
  await client.query(`
    TRUNCATE TABLE
      train_observations,
      train_snapshots,
      train_daily_stats,
      trains_current,
      stations,
      ingestion_batches,
      ingestion_runs,
      app_state
    RESTART IDENTITY CASCADE;
  `);
};

const normalizeValue = (value: unknown) => {
  if (typeof value === "bigint") {
    return Number(value);
  }

  return value ?? null;
};

const insertRows = async (
  client: Client,
  tableName: string,
  columns: string[],
  rows: SqlRow[],
) => {
  if (rows.length === 0) {
    return;
  }

  for (let i = 0; i < rows.length; i += batchSize) {
    const chunk = rows.slice(i, i + batchSize);
    const values: unknown[] = [];
    const tuples: string[] = [];
    let placeholder = 1;

    for (const row of chunk) {
      const tuple: string[] = [];
      for (const column of columns) {
        tuple.push(`$${placeholder}`);
        values.push(normalizeValue(row[column]));
        placeholder += 1;
      }
      tuples.push(`(${tuple.join(",")})`);
    }

    const sql = `INSERT INTO ${tableName} (${columns.join(",")}) VALUES ${tuples.join(",")}`;
    await client.query(sql, values);
  }
};

const setSequences = async (client: Client) => {
  await client.query(`
    SELECT setval(pg_get_serial_sequence('ingestion_runs','id'), COALESCE((SELECT MAX(id) FROM ingestion_runs), 1), true);
    SELECT setval(pg_get_serial_sequence('ingestion_batches','id'), COALESCE((SELECT MAX(id) FROM ingestion_batches), 1), true);
    SELECT setval(pg_get_serial_sequence('train_snapshots','id'), COALESCE((SELECT MAX(id) FROM train_snapshots), 1), true);
    SELECT setval(pg_get_serial_sequence('train_observations','id'), COALESCE((SELECT MAX(id) FROM train_observations), 1), true);
  `);
};

const copyTable = async (
  client: Client,
  sourceDb: Database,
  tableName: string,
  columns: string[],
  orderBy?: string,
) => {
  const sql = `SELECT ${columns.join(",")} FROM ${tableName}${orderBy ? ` ORDER BY ${orderBy}` : ""}`;
  const rows = sourceDb.query(sql).all() as SqlRow[];

  await insertRows(client, tableName, columns, rows);
  console.log(`[migrate] ${tableName}: ${rows.length} filas`);
};

const run = async () => {
  const startedAt = nowEpoch();
  console.log(`[migrate] inicio sqlite=${sqlitePath}`);

  await pg.connect();

  try {
    await createSchema(pg);
    await pg.query("BEGIN");
    await truncateTarget(pg);

    await copyTable(pg, sqlite, "app_state", ["key", "value", "updated_at"], "key");
    await copyTable(
      pg,
      sqlite,
      "ingestion_runs",
      [
        "id",
        "fetched_at",
        "source",
        "success",
        "train_count",
        "skipped",
        "error",
        "provider_updated_at",
      ],
      "id",
    );
    await copyTable(
      pg,
      sqlite,
      "ingestion_batches",
      ["id", "fetched_at", "source", "provider_updated_at", "train_count", "payload_hash", "created_at"],
      "id",
    );
    await copyTable(
      pg,
      sqlite,
      "stations",
      [
        "code",
        "name",
        "locality",
        "province",
        "accessible",
        "attended",
        "correspondences",
        "level",
        "lat",
        "lon",
        "updated_at",
      ],
      "code",
    );
    await copyTable(
      pg,
      sqlite,
      "trains_current",
      [
        "cod_comercial",
        "cod_product",
        "cod_origen",
        "cod_destino",
        "cod_est_ant",
        "cod_est_sig",
        "hora_llegada_sig_est",
        "des_corridor",
        "accesible",
        "ult_retraso",
        "latitud",
        "longitud",
        "gps_time",
        "p",
        "mat",
        "first_seen_at",
        "last_seen_at",
        "last_payload_hash",
        "last_snapshot_at",
      ],
      "cod_comercial",
    );
    await copyTable(
      pg,
      sqlite,
      "train_snapshots",
      [
        "id",
        "cod_comercial",
        "captured_at",
        "cod_product",
        "cod_origen",
        "cod_destino",
        "cod_est_ant",
        "cod_est_sig",
        "hora_llegada_sig_est",
        "des_corridor",
        "accesible",
        "ult_retraso",
        "latitud",
        "longitud",
        "gps_time",
        "p",
        "mat",
        "hash",
      ],
      "id",
    );
    await copyTable(
      pg,
      sqlite,
      "train_daily_stats",
      [
        "day",
        "cod_comercial",
        "cod_product",
        "cod_origen",
        "cod_destino",
        "des_corridor",
        "observations",
        "ahead_count",
        "on_time_count",
        "mild_count",
        "medium_count",
        "severe_count",
        "avg_delay",
        "max_delay",
        "min_delay",
        "total_distance_km",
        "first_seen_at",
        "last_seen_at",
      ],
      "day, cod_comercial",
    );
    await copyTable(
      pg,
      sqlite,
      "train_observations",
      [
        "id",
        "batch_id",
        "cod_comercial",
        "captured_at",
        "cod_product",
        "cod_origen",
        "cod_destino",
        "cod_est_ant",
        "cod_est_sig",
        "hora_llegada_sig_est",
        "des_corridor",
        "accesible",
        "ult_retraso",
        "latitud",
        "longitud",
        "gps_time",
        "p",
        "mat",
        "hash",
        "source",
        "is_estimated",
      ],
      "id",
    );

    await setSequences(pg);
    await pg.query("COMMIT");

    const finishedAt = nowEpoch();
    console.log(`[migrate] completado en ${finishedAt - startedAt}s`);
  } catch (error) {
    await pg.query("ROLLBACK");
    throw error;
  } finally {
    sqlite.close();
    await pg.end();
  }
};

await run();
