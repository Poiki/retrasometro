import { existsSync } from "node:fs";
import { Database } from "bun:sqlite";
import { Client } from "pg";

type SqlRow = Record<string, unknown>;

const sqlitePath = Bun.env.SQLITE_PATH ?? "./data/renfe.db";
const postgresUrl = Bun.env.POSTGRES_URL ?? Bun.env.DATABASE_URL;
const batchSize = Number(Bun.env.MIGRATION_BATCH_SIZE ?? 500);
if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > 1000) throw new Error("MIGRATION_BATCH_SIZE debe ser un entero entre 1 y 1000.");

if (!postgresUrl) {
  throw new Error("Define POSTGRES_URL o DATABASE_URL para ejecutar la migracion.");
}

if (!existsSync(sqlitePath)) {
  throw new Error(`No existe la base sqlite en ${sqlitePath}`);
}

const sqlite = new Database(sqlitePath, { readonly: true, safeIntegers: true });
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

    CREATE TABLE IF NOT EXISTS observation_archive_chunks (
      id BIGSERIAL PRIMARY KEY,
      from_ts BIGINT NOT NULL,
      to_ts BIGINT NOT NULL,
      row_count INTEGER NOT NULL,
      file_path TEXT NOT NULL UNIQUE,
      sha256 TEXT NOT NULL,
      created_at BIGINT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS train_hourly_train_stats (
      hour_epoch BIGINT NOT NULL,
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
      sum_delay DOUBLE PRECISION NOT NULL DEFAULT 0,
      sum_positive_delay INTEGER NOT NULL DEFAULT 0,
      max_delay INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY(hour_epoch, cod_comercial)
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

      CREATE TABLE IF NOT EXISTS ops_service_hourly (
        service_instance_id TEXT NOT NULL,
        hour_epoch BIGINT NOT NULL,
        operational_day TEXT NOT NULL,
        cod_comercial TEXT NOT NULL,
        cod_product BIGINT NOT NULL,
        cod_origen TEXT,
        cod_destino TEXT,
        corridor_direction TEXT NOT NULL,
        observations_total BIGINT NOT NULL DEFAULT 0,
        observations_live BIGINT NOT NULL DEFAULT 0,
        observations_recovered BIGINT NOT NULL DEFAULT 0,
        observations_repeated BIGINT NOT NULL DEFAULT 0,
        max_delay_live BIGINT NOT NULL DEFAULT 0,
        max_delay_all BIGINT NOT NULL DEFAULT 0,
        tm_total_sec BIGINT NOT NULL DEFAULT 0,
        tm_gt0_sec BIGINT NOT NULL DEFAULT 0,
        tm_gt15_sec BIGINT NOT NULL DEFAULT 0,
        tm_gt60_sec BIGINT NOT NULL DEFAULT 0,
        first_ts BIGINT NOT NULL,
        last_ts BIGINT NOT NULL,
        first_live_ts BIGINT,
        last_live_ts BIGINT,
        first_live_delay BIGINT,
        last_live_delay BIGINT,
        official_corridor_obs BIGINT NOT NULL DEFAULT 0,
        synthetic_id_obs BIGINT NOT NULL DEFAULT 0,
        origin_station_match_obs BIGINT NOT NULL DEFAULT 0,
        destination_station_match_obs BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY(service_instance_id, hour_epoch)
      );
      CREATE INDEX IF NOT EXISTS idx_ops_hourly_time ON ops_service_hourly(hour_epoch DESC);
      CREATE INDEX IF NOT EXISTS idx_ops_hourly_direction_time ON ops_service_hourly(corridor_direction, hour_epoch DESC);
      CREATE INDEX IF NOT EXISTS idx_ops_hourly_product_time ON ops_service_hourly(cod_product, hour_epoch DESC);
      CREATE INDEX IF NOT EXISTS idx_ops_hourly_operational_day ON ops_service_hourly(operational_day DESC);

      CREATE TABLE IF NOT EXISTS ops_materialization_state (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at BIGINT NOT NULL
      );

    CREATE INDEX IF NOT EXISTS idx_ingestion_fetched_at ON ingestion_runs(fetched_at DESC);
    CREATE INDEX IF NOT EXISTS idx_batches_fetched_at ON ingestion_batches(fetched_at DESC);
    CREATE INDEX IF NOT EXISTS idx_current_product ON trains_current(cod_product);
    CREATE INDEX IF NOT EXISTS idx_current_last_seen ON trains_current(last_seen_at DESC);
    CREATE INDEX IF NOT EXISTS idx_current_delay ON trains_current(ult_retraso DESC);
    CREATE INDEX IF NOT EXISTS idx_snapshots_train_time ON train_snapshots(cod_comercial, captured_at DESC);
    CREATE INDEX IF NOT EXISTS idx_snapshots_time ON train_snapshots(captured_at DESC);
    CREATE INDEX IF NOT EXISTS idx_observations_time ON train_observations(captured_at DESC);
    CREATE INDEX IF NOT EXISTS idx_archive_chunks_range ON observation_archive_chunks(from_ts, to_ts);
    CREATE INDEX IF NOT EXISTS idx_hourly_time ON train_hourly_train_stats(hour_epoch DESC);
    CREATE INDEX IF NOT EXISTS idx_hourly_product_time ON train_hourly_train_stats(cod_product, hour_epoch DESC);
    CREATE INDEX IF NOT EXISTS idx_hourly_corridor_time ON train_hourly_train_stats(des_corridor, hour_epoch DESC);
    CREATE INDEX IF NOT EXISTS idx_hourly_axis_time ON train_hourly_train_stats(cod_origen, cod_destino, hour_epoch DESC);
    CREATE INDEX IF NOT EXISTS idx_daily_day ON train_daily_stats(day DESC);
    CREATE INDEX IF NOT EXISTS idx_daily_product_day ON train_daily_stats(cod_product, day DESC);
  `);
};

const normalizeValue = (value: unknown) => {
  if (typeof value === "bigint") {
    return value.toString();
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

  const values = rows.flatMap((row) => columns.map((column) => normalizeValue(row[column])));
  const tuples = rows.map((_, rowIndex) => `(${columns.map((_, columnIndex) => `$${rowIndex * columns.length + columnIndex + 1}`).join(",")})`);
  const columnNames = columns.map((column) => `"${column.replaceAll('"', '""')}"`).join(",");
  await client.query(`INSERT INTO ${tableName} (${columnNames}) VALUES ${tuples.join(",")}`, values);
};

const setSequences = async (client: Client) => {
  await client.query(`
    SELECT setval(pg_get_serial_sequence('ingestion_runs','id'), COALESCE((SELECT MAX(id) FROM ingestion_runs), 1), true);
    SELECT setval(pg_get_serial_sequence('ingestion_batches','id'), COALESCE((SELECT MAX(id) FROM ingestion_batches), 1), true);
    SELECT setval(pg_get_serial_sequence('train_snapshots','id'), COALESCE((SELECT MAX(id) FROM train_snapshots), 1), true);
    SELECT setval(pg_get_serial_sequence('train_observations','id'), COALESCE((SELECT MAX(id) FROM train_observations), 1), true);
    SELECT setval(pg_get_serial_sequence('observation_archive_chunks','id'), COALESCE((SELECT MAX(id) FROM observation_archive_chunks), 1), true);
  `);
};

const copyTable = async (
  client: Client,
  sourceDb: Database,
  tableName: string,
  columns: string[],
) => {
  const columnNames = columns.map((column) => `"${column.replaceAll('"', '""')}"`).join(",");
  const sql = `SELECT ${columnNames} FROM ${tableName}`;
  let chunk: SqlRow[] = [];
  let count = 0;
  for (const row of sourceDb.query(sql).iterate()) {
    chunk.push(row as SqlRow);
    if (chunk.length === batchSize) {
      await insertRows(client, tableName, columns, chunk);
      count += chunk.length;
      chunk = [];
    }
  }
  await insertRows(client, tableName, columns, chunk);
  count += chunk.length;
  const target = await client.query(`SELECT COUNT(*) AS n FROM ${tableName}`);
  if (Number(target.rows[0].n) !== count) throw new Error(`Recuento incorrecto: ${tableName}`);
  console.log(`[migrate] ${tableName}: ${count} filas`);
};

const run = async () => {
  const startedAt = nowEpoch();
  console.log(`[migrate] inicio sqlite=${sqlitePath}`);

  await pg.connect();

  try {
    await pg.query("BEGIN");
    sqlite.exec("BEGIN");
    await createSchema(pg);
    const tables = ["app_state", "ingestion_runs", "ingestion_batches", "stations", "trains_current",
      "train_snapshots", "train_hourly_train_stats", "train_daily_stats", "observation_archive_chunks",
      "train_observations", "ops_service_hourly", "ops_materialization_state"];
    for (const table of tables) {
      const existing = await pg.query(`SELECT 1 FROM ${table} LIMIT 1`);
      if (existing.rows.length) throw new Error(`Destino no vacío: ${table}. Usa una base nueva; no se sobrescriben datos.`);
    }
    for (const table of tables) {
      const columns = (sqlite.query(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>).map((row) => row.name);
      if (columns.length) await copyTable(pg, sqlite, table, columns);
    }
    console.log("[migrate] Los JSONL.zst deben conservarse junto a la base: se copian sus rutas y manifiesto, no los archivos.");

    await setSequences(pg);
    await pg.query("COMMIT");

    const finishedAt = nowEpoch();
    console.log(`[migrate] completado en ${finishedAt - startedAt}s`);
  } catch (error) {
    await pg.query("ROLLBACK");
    throw error;
  } finally {
    if (sqlite.inTransaction) sqlite.exec("ROLLBACK");
    sqlite.close();
    await pg.end();
  }
};

await run();
