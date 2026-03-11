const parseNumber = (value: string | undefined, fallback: number) => {
  if (!value) {
    return fallback;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
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
  cacheFile: Bun.env.CACHE_FILE ?? "./data/cache/flotaLD.latest.json",
  apiKeyTtlSeconds: parseNumber(Bun.env.API_KEY_TTL_SECONDS, 900),
  apiRateLimitMs: parseNumber(Bun.env.API_RATE_LIMIT_MS, 1000),
  rawMaxTrains: parseNumber(Bun.env.RAW_MAX_TRAINS, 5000),
  historyRetentionDays: parseNumber(Bun.env.HISTORY_RETENTION_DAYS, 0),
  recoveryLookbackHours: parseNumber(Bun.env.RECOVERY_LOOKBACK_HOURS, 72),
};
