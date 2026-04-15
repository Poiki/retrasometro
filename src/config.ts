const parseNumber = (value: string | undefined, fallback: number) => {
  if (!value) {
    return fallback;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const parseBoolean = (value: string | undefined, fallback: boolean) => {
  if (!value) {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }

  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }

  return fallback;
};

const parseList = (value: string | undefined, fallback: string[]) => {
  if (!value) {
    return fallback;
  }

  return value
    .split(",")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
};

export const config = {
  port: parseNumber(Bun.env.PORT, 3000),
  dbPath: Bun.env.DB_PATH ?? "./data/renfe.db",
  pollingIntervalMs: parseNumber(Bun.env.POLL_INTERVAL_MS, 60_000),
  fetchTimeoutMs: parseNumber(Bun.env.FETCH_TIMEOUT_MS, 12_000),
  staleTrainSeconds: parseNumber(Bun.env.STALE_TRAIN_SECONDS, 180),
  snapshotRetentionHours: parseNumber(Bun.env.SNAPSHOT_RETENTION_HOURS, 96),
  snapshotHeartbeatSeconds: parseNumber(Bun.env.SNAPSHOT_HEARTBEAT_SECONDS, 300),
  compactEveryRuns: parseNumber(Bun.env.COMPACT_EVERY_RUNS, 15),
  stationsRefreshHours: parseNumber(Bun.env.STATIONS_REFRESH_HOURS, 24),
  endpointList: parseList(Bun.env.RENFE_ENDPOINTS, [
    "https://tiempo-real.largorecorrido.renfe.com/renfe-visor/flotaLD.json",
  ]),
  stationsEndpoint:
    Bun.env.STATIONS_ENDPOINT ??
    "https://tiempo-real.largorecorrido.renfe.com/data/estaciones.geojson",
  stationsSupplementalEndpoint:
    Bun.env.STATIONS_SUPPLEMENTAL_ENDPOINT ??
    "https://tiempo-real.renfe.com/data/estaciones.geojson?v=1",
  cacheFile: Bun.env.CACHE_FILE ?? "./data/cache/flotaLD.latest.json",
  displayCacheDir: Bun.env.DISPLAY_CACHE_DIR ?? "./data/cache/display",
  displayCache24Seconds: parseNumber(Bun.env.DISPLAY_CACHE_24_SECONDS, 60),
  displayCache168Seconds: parseNumber(Bun.env.DISPLAY_CACHE_168_SECONDS, 300),
  displayCache720Seconds: parseNumber(Bun.env.DISPLAY_CACHE_720_SECONDS, 900),
  apiKeyTtlSeconds: parseNumber(Bun.env.API_KEY_TTL_SECONDS, 900),
  apiRateLimitMs: parseNumber(Bun.env.API_RATE_LIMIT_MS, 200),
  rawMaxTrains: parseNumber(Bun.env.RAW_MAX_TRAINS, 5000),
  observationRetentionDays: parseNumber(
    Bun.env.OBSERVATION_RETENTION_DAYS ?? Bun.env.HISTORY_RETENTION_DAYS,
    14,
  ),
  hourlyRetentionDays: parseNumber(Bun.env.HOURLY_RETENTION_DAYS, 400),
  dailyRetentionDays: parseNumber(Bun.env.DAILY_RETENTION_DAYS, 0),
  batchRetentionDays: parseNumber(Bun.env.BATCH_RETENTION_DAYS, 14),
  archiveEnabled: parseBoolean(Bun.env.ARCHIVE_ENABLED, true),
  archiveDir: Bun.env.ARCHIVE_DIR ?? "./data/archive/observations",
  archiveZstdLevel: parseNumber(Bun.env.ARCHIVE_ZSTD_LEVEL, 3),
  walCheckpointTruncateBytes: parseNumber(Bun.env.WAL_CHECKPOINT_TRUNCATE_BYTES, 64 * 1024 * 1024),
  httpCompressionEnabled: parseBoolean(Bun.env.HTTP_COMPRESSION_ENABLED, true),
  httpCompressionMinBytes: parseNumber(Bun.env.HTTP_COMPRESSION_MIN_BYTES, 1024),
  httpGzipLevel: parseNumber(Bun.env.HTTP_GZIP_LEVEL, 5),
  httpBrotliLevel: parseNumber(Bun.env.HTTP_BROTLI_LEVEL, 4),
  recoveryLookbackHours: parseNumber(Bun.env.RECOVERY_LOOKBACK_HOURS, 72),
};
