import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { config } from "./config";
import { DB } from "./db";
import type {
  CurrentTrainRow,
  NormalizedTrain,
  StationRecord,
  TrainsPayload,
} from "./types";
import {
  computeSnapshotHash,
  getDayString,
  haversineKm,
  normalizeTrain,
  toEpochSeconds,
} from "./utils";

interface IngestStatus {
  isRunning: boolean;
  lastStartAt: number | null;
  lastFinishAt: number | null;
  lastSuccessAt: number | null;
  lastError: string | null;
  lastSource: string | null;
  lastProviderUpdatedAt: string | null;
  lastTrainCount: number;
  runs: number;
}

export class RenfeIngestor {
  private readonly db: DB;
  private isTickRunning = false;
  private runTimer: Timer | null = null;
  private stationTimer: Timer | null = null;
  private runCounter = 0;
  private status: IngestStatus = {
    isRunning: false,
    lastStartAt: null,
    lastFinishAt: null,
    lastSuccessAt: null,
    lastError: null,
    lastSource: null,
    lastProviderUpdatedAt: null,
    lastTrainCount: 0,
    runs: 0,
  };

  constructor(db: DB) {
    this.db = db;
    mkdirSync(dirname(config.cacheFile), { recursive: true });
  }

  async start() {
    await this.refreshStations();
    await this.runOnce();
    await this.recoverHistoricalData();

    this.runTimer = setInterval(() => {
      void this.runOnce();
    }, config.pollingIntervalMs);

    this.stationTimer = setInterval(() => {
      void this.refreshStations();
    }, config.stationsRefreshHours * 3600 * 1000);
  }

  stop() {
    if (this.runTimer) {
      clearInterval(this.runTimer);
      this.runTimer = null;
    }

    if (this.stationTimer) {
      clearInterval(this.stationTimer);
      this.stationTimer = null;
    }
  }

  getStatus() {
    return {
      ...this.status,
      pollingIntervalMs: config.pollingIntervalMs,
      staleTrainSeconds: config.staleTrainSeconds,
      snapshotRetentionHours: config.snapshotRetentionHours,
      endpointList: config.endpointList,
      stationsEndpoint: config.stationsEndpoint,
    };
  }

  private async fetchPayload(): Promise<{ payload: TrainsPayload; source: string }> {
    const failures: string[] = [];

    for (const endpoint of config.endpointList) {
      const glue = endpoint.includes("?") ? "&" : "?";
      const url = `${endpoint}${glue}v=${Date.now()}`;

      try {
        const response = await fetch(url, {
          signal: AbortSignal.timeout(config.fetchTimeoutMs),
          headers: {
            "cache-control": "no-cache",
          },
        });

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const payload = (await response.json()) as TrainsPayload;

        if (!payload || !Array.isArray(payload.trenes)) {
          throw new Error("JSON inválido o sin campo trenes");
        }

        await Bun.write(config.cacheFile, JSON.stringify(payload));

        return {
          payload,
          source: endpoint,
        };
      } catch (error) {
        failures.push(`${endpoint}: ${error instanceof Error ? error.message : String(error)}`);
      }
    }

    const cache = Bun.file(config.cacheFile);
    if (await cache.exists()) {
      const payload = (await cache.json()) as TrainsPayload;
      if (payload && Array.isArray(payload.trenes)) {
        return {
          payload,
          source: "cache-local",
        };
      }
    }

    throw new Error(`No se pudo obtener flotaLD.json (${failures.join(" | ")})`);
  }

  async runOnce() {
    if (this.isTickRunning) {
      return;
    }

    this.isTickRunning = true;
    this.status.isRunning = true;
    this.status.lastStartAt = toEpochSeconds();

    const nowEpoch = toEpochSeconds();

    try {
      const { payload, source } = await this.fetchPayload();
      const providerUpdatedAt = payload.fechaActualizacion ?? null;
      const lastProviderUpdatedAt = this.db.getState("last_provider_updated_at");
      const isRepeatedProviderUpdate =
        Boolean(providerUpdatedAt) && providerUpdatedAt === lastProviderUpdatedAt;

      const rawTrains = Array.isArray(payload.trenes) ? payload.trenes : [];
      const trainMap = new Map<string, NormalizedTrain>();

      for (const raw of rawTrains) {
        const train = normalizeTrain(raw);
        if (train) {
          trainMap.set(train.codComercial, train);
        }
      }

      const normalized = [...trainMap.values()];
      const day = getDayString(nowEpoch);
      const payloadHash = Bun.hash(JSON.stringify(payload)).toString();
      const batchId = this.db.createIngestionBatch({
        fetchedAt: nowEpoch,
        source,
        providerUpdatedAt,
        trainCount: normalized.length,
        payloadHash,
      });

      let snapshotCount = 0;
      const observationItems: Array<{ train: NormalizedTrain; hash: string }> = [];

      for (const train of normalized) {
        const existing = this.db.getCurrentTrain(train.codComercial);
        const hash = computeSnapshotHash(train);
        const shouldSnapshot = this.shouldSnapshot(existing, hash, nowEpoch);
        const snapshotAt = shouldSnapshot ? nowEpoch : null;
        observationItems.push({ train, hash });

        this.db.upsertCurrentTrain({
          train,
          nowEpoch,
          hash,
          snapshotAt,
        });

        if (shouldSnapshot) {
          this.db.insertSnapshot(train, nowEpoch, hash);
          snapshotCount += 1;
        }

        const distanceKm = this.computeDistance(existing, train);

        this.db.upsertDailyStats({
          train,
          day,
          nowEpoch,
          distanceKm,
        });
      }

      this.db.insertTrainObservations(
        batchId,
        nowEpoch,
        observationItems,
        isRepeatedProviderUpdate ? "live_repeated_provider_ts" : "live",
        0,
      );

      const removed = this.cleanup(nowEpoch);

      this.db.recordIngestionRun({
        fetchedAt: nowEpoch,
        source,
        success: 1,
        trainCount: normalized.length,
        skipped: isRepeatedProviderUpdate ? 1 : 0,
        error: null,
        providerUpdatedAt,
      });

      if (providerUpdatedAt) {
        this.db.setState("last_provider_updated_at", providerUpdatedAt);
      }

      this.status.lastSuccessAt = nowEpoch;
      this.status.lastError = null;
      this.status.lastSource = source;
      this.status.lastProviderUpdatedAt = providerUpdatedAt;
      this.status.lastTrainCount = normalized.length;

      this.runCounter += 1;

      console.log(
        `[ingestor] ok trains=${normalized.length} snapshots=${snapshotCount} removed=${removed} source=${source} repeated=${isRepeatedProviderUpdate ? 1 : 0}`,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.status.lastError = message;

      this.db.recordIngestionRun({
        fetchedAt: nowEpoch,
        source: "none",
        success: 0,
        trainCount: 0,
        skipped: 0,
        error: message,
        providerUpdatedAt: null,
      });

      console.error(`[ingestor] error ${message}`);
    } finally {
      this.status.isRunning = false;
      this.status.lastFinishAt = toEpochSeconds();
      this.status.runs += 1;
      this.isTickRunning = false;
    }
  }

  private shouldSnapshot(
    existing: CurrentTrainRow | null,
    hash: string,
    nowEpoch: number,
  ): boolean {
    if (!existing) {
      return true;
    }

    if (!existing.last_snapshot_at) {
      return true;
    }

    if (existing.last_payload_hash !== hash) {
      return true;
    }

    return nowEpoch - existing.last_snapshot_at >= config.snapshotHeartbeatSeconds;
  }

  private computeDistance(existing: CurrentTrainRow | null, incoming: NormalizedTrain): number {
    if (!existing) {
      return 0;
    }

    const distance = haversineKm(
      existing.latitud,
      existing.longitud,
      incoming.latitud,
      incoming.longitud,
    );

    if (!Number.isFinite(distance) || distance < 0) {
      return 0;
    }

    // Descarta saltos no realistas por errores de geolocalizacion.
    return distance <= 15 ? distance : 0;
  }

  private cleanup(nowEpoch: number): number {
    const staleCutoff = nowEpoch - config.staleTrainSeconds;
    const removed = this.db.deleteStaleCurrentTrains(staleCutoff);

    if (this.runCounter % config.compactEveryRuns === 0) {
      const snapshotsCutoff = nowEpoch - config.snapshotRetentionHours * 3600;
      const runsCutoff = nowEpoch - 30 * 24 * 3600;

      const deletedSnapshots = this.db.cleanupSnapshots(snapshotsCutoff);
      this.db.cleanupIngestionRuns(runsCutoff);

      if (config.historyRetentionDays > 0) {
        const historyCutoff = nowEpoch - config.historyRetentionDays * 24 * 3600;
        this.db.cleanupObservations(historyCutoff);
        this.db.cleanupBatches(historyCutoff);
      }

      this.db.optimize();

      if (deletedSnapshots > 0) {
        console.log(`[compact] snapshots eliminados=${deletedSnapshots}`);
      }
    }

    return removed;
  }

  private async recoverHistoricalData() {
    const sinceEpoch = toEpochSeconds() - config.recoveryLookbackHours * 3600;
    const recovered = this.db.recoverObservationsFromSnapshots(sinceEpoch);

    if (recovered > 0) {
      console.log(`[recovery] observaciones recuperadas desde snapshots=${recovered}`);
    }
  }

  async refreshStations() {
    try {
      const response = await fetch(config.stationsEndpoint, {
        signal: AbortSignal.timeout(config.fetchTimeoutMs),
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = (await response.json()) as {
        features?: Array<{
          properties?: Record<string, unknown>;
          geometry?: { coordinates?: [number, number] };
        }>;
      };

      if (!Array.isArray(data.features)) {
        throw new Error("GeoJSON de estaciones sin features");
      }

      const stations: StationRecord[] = [];

      for (const feature of data.features) {
        const props = feature.properties ?? {};
        const code = this.asCode(props.CODIGO);
        if (!code) {
          continue;
        }

        const coordinates = feature.geometry?.coordinates;
        stations.push({
          code,
          name: this.asText(props.NOMBRE),
          locality: this.asText(props.LOCALIDAD),
          province: this.asText(props.PROV),
          accessible: this.asFlag(props.ACCESIBLE),
          attended: this.asFlag(props.ATENDO),
          correspondences: this.asText(props.CERC),
          level: this.asText(props.NIVEL),
          lat: Array.isArray(coordinates) ? this.asNumber(coordinates[1]) : null,
          lon: Array.isArray(coordinates) ? this.asNumber(coordinates[0]) : null,
        });
      }

      const nowEpoch = toEpochSeconds();
      this.db.upsertStations(stations, nowEpoch);
      this.db.setState("stations_last_refresh_at", String(nowEpoch));

      console.log(`[stations] actualizadas ${stations.length}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[stations] error ${message}`);
    }
  }

  private asCode(value: unknown): string | null {
    if (typeof value === "number" && Number.isFinite(value)) {
      return Math.trunc(value).toString().padStart(5, "0");
    }

    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed.length === 0) {
        return null;
      }

      if (/^\d+$/.test(trimmed)) {
        return trimmed.padStart(5, "0");
      }

      return trimmed;
    }

    return null;
  }

  private asText(value: unknown): string | null {
    if (typeof value !== "string") {
      if (typeof value === "number" && Number.isFinite(value)) {
        return String(value);
      }
      return null;
    }

    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }

  private asNumber(value: unknown): number | null {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }

    if (typeof value === "string") {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : null;
    }

    return null;
  }

  private asFlag(value: unknown): number | null {
    if (typeof value === "boolean") {
      return value ? 1 : 0;
    }

    if (typeof value === "number") {
      return value > 0 ? 1 : 0;
    }

    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      if (["1", "s", "si", "yes", "true"].includes(normalized)) {
        return 1;
      }
      if (["0", "n", "no", "false"].includes(normalized)) {
        return 0;
      }
    }

    return null;
  }
}
