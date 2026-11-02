import { buildServiceInstanceId, getOperationalDayMadrid, haversineKm } from "./utils";

export interface MapObservation {
  cod_comercial: string;
  cod_product: number;
  cod_origen: string | null;
  cod_destino: string | null;
  captured_at: number;
  gps_time: number | null;
  latitud: number;
  longitud: number;
  ult_retraso: number;
  source: string;
  is_estimated: number;
}

const MAX_GAP_SECONDS = 180;
const MAX_SPEED_KMH = 400;
const CELL_DEGREES = 0.025;

export interface MapDelayCell {
  id: string;
  bounds: number[][];
  samples: number;
  delayIntervals: number;
  gainedDelayMinutes: number;
  delayedServices: string[];
  firstDelayAt: number | null;
  lastDelayAt: number | null;
  maxDelayMinutes: number;
}

export const calculateMapDelayZones = (cells: Iterable<MapDelayCell>) => {
  const merged = new Map<string, MapDelayCell & { services: Set<string> }>();
  for (const cell of cells) {
    let total = merged.get(cell.id);
    if (!total) {
      total = { ...cell, samples: 0, delayIntervals: 0, gainedDelayMinutes: 0,
        delayedServices: [], services: new Set() };
      merged.set(cell.id, total);
    }
    total.samples += cell.samples;
    total.delayIntervals += cell.delayIntervals;
    total.gainedDelayMinutes += cell.gainedDelayMinutes;
    for (const service of cell.delayedServices) total.services.add(service);
    if (cell.firstDelayAt !== null) total.firstDelayAt = Math.min(total.firstDelayAt ?? Infinity, cell.firstDelayAt);
    if (cell.lastDelayAt !== null) total.lastDelayAt = Math.max(total.lastDelayAt ?? 0, cell.lastDelayAt);
    total.maxDelayMinutes = Math.max(total.maxDelayMinutes, cell.maxDelayMinutes);
  }
  return [...merged.values()]
    .filter((cell) => cell.delayIntervals >= 3 && cell.services.size >= 2 && cell.delayIntervals / cell.samples >= 0.2)
    .map(({ services, delayedServices, ...cell }) => ({ ...cell, trainCount: services.size,
      averageAddedDelayMinutes: Math.round(cell.gainedDelayMinutes / services.size * 10) / 10 }))
    .sort((a, b) => b.gainedDelayMinutes - a.gainedDelayMinutes || b.trainCount - a.trainCount);
};

export const calculateMapAnalytics = (rows: Iterable<MapObservation>, untilEpoch: number, sinceEpoch = untilEpoch - 86400) => {
  const services = new Map<string, {
    row: MapObservation; previous: MapObservation | null; previousGps: number; speed: number | null;
    distanceKm: number; seconds: number; delaySeconds: number; intervals: number;
    gainedDelayMinutes: number; lastGpsAt: number;
  }>();
  const cells = new Map<string, MapDelayCell & { services: Set<string> }>();
  let rejectedSamples = 0;
  let repeatedSamples = 0;

  // Rows arrive in capture order from the indexed database query.
  for (const row of rows) {
    if (row.captured_at < sinceEpoch - MAX_GAP_SECONDS || row.captured_at > untilEpoch) continue;
    const id = buildServiceInstanceId(row.cod_comercial, getOperationalDayMadrid(row.captured_at), row.cod_origen, row.cod_destino) + `|${row.cod_product}`;
    let service = services.get(id);
    if (!service) {
      service = { row, previous: null, previousGps: 0, speed: null, distanceKm: 0, seconds: 0,
        delaySeconds: 0, intervals: 0, gainedDelayMinutes: 0, lastGpsAt: 0 };
      services.set(id, service);
    }
    const gps = row.gps_time !== null && row.gps_time > 1e12 ? row.gps_time / 1000 : row.gps_time;
    if (row.is_estimated || !["live", "live_repeated_provider_ts"].includes(row.source) ||
      gps === null || !Number.isFinite(gps) || Math.abs(row.captured_at - gps) > MAX_GAP_SECONDS ||
      !Number.isFinite(row.latitud) || !Number.isFinite(row.longitud) ||
      Math.abs(row.latitud) > 85.05112878 || Math.abs(row.longitud) > 180 ||
      (row.latitud === 0 && row.longitud === 0) || !Number.isFinite(row.ult_retraso)) {
      service.previous = null;
      service.speed = null;
      rejectedSamples++;
      continue;
    }
    if (service.previous && gps <= service.previousGps) {
      repeatedSamples++;
      continue;
    }
    const previous = service.previous;
    const elapsed = gps - service.previousGps;
    service.previous = row;
    service.previousGps = gps;
    service.lastGpsAt = gps;
    service.row = row;
    if (!previous) continue;
    if (row.captured_at < sinceEpoch) continue;
    if (elapsed > MAX_GAP_SECONDS || row.captured_at - previous.captured_at > MAX_GAP_SECONDS ||
      row.captured_at <= previous.captured_at) {
      service.speed = null;
      rejectedSamples++;
      continue;
    }
    let distanceKm = haversineKm(previous.latitud, previous.longitud, row.latitud, row.longitud);
    if (distanceKm < 0.05) distanceKm = 0;
    const speed = distanceKm / elapsed * 3600;
    if (!Number.isFinite(speed) || speed > MAX_SPEED_KMH) {
      service.previous = null;
      service.speed = null;
      rejectedSamples++;
      continue;
    }
    const increase = Math.max(0, row.ult_retraso - Math.max(0, previous.ult_retraso));
    // Larger increases cannot be attributed to time physically lost within this sampled interval.
    const gainedDelay = increase <= elapsed / 60 + 1 ? increase : 0;
    service.speed = speed;
    service.distanceKm += distanceKm;
    service.seconds += elapsed;
    service.delaySeconds += row.ult_retraso * elapsed;
    service.intervals++;
    service.gainedDelayMinutes += gainedDelay;

    // shortcut: cells approximate GPS interval locations; use rail map matching for track-level attribution.
    const lat = Math.floor((previous.latitud + row.latitud) / 2 / CELL_DEGREES) * CELL_DEGREES;
    const lon = Math.floor((previous.longitud + row.longitud) / 2 / CELL_DEGREES) * CELL_DEGREES;
    const cellId = `${lat.toFixed(3)},${lon.toFixed(3)}`;
    let cell = cells.get(cellId);
    if (!cell) {
      cell = { id: cellId, bounds: [[lat, lon], [lat + CELL_DEGREES, lon + CELL_DEGREES]],
        samples: 0, delayIntervals: 0, gainedDelayMinutes: 0, delayedServices: [], services: new Set(),
        firstDelayAt: null, lastDelayAt: null, maxDelayMinutes: 0 };
      cells.set(cellId, cell);
    }
    cell.samples++;
    if (gainedDelay >= 1) {
      cell.services.add(id);
      cell.delayIntervals++;
      cell.gainedDelayMinutes += gainedDelay;
      cell.firstDelayAt = Math.min(cell.firstDelayAt ?? Infinity, gps);
      cell.lastDelayAt = Math.max(cell.lastDelayAt ?? 0, gps);
      cell.maxDelayMinutes = Math.max(cell.maxDelayMinutes, row.ult_retraso);
    }
  }

  let validIntervals = 0;
  const latestServices = new Map<string, ReturnType<typeof toTrain>>();
  function toTrain(service: NonNullable<ReturnType<typeof services.get>>) {
    const row = service.row;
    return {
      codComercial: row.cod_comercial, codProduct: row.cod_product, codOrigen: row.cod_origen, codDestino: row.cod_destino,
      averageSpeedKmh: service.seconds ? Math.round(service.distanceKm / service.seconds * 36000) / 10 : null,
      latestSpeedKmh: service.speed !== null && untilEpoch - service.lastGpsAt <= MAX_GAP_SECONDS ? Math.round(service.speed * 10) / 10 : null,
      averageDelayMinutes: service.seconds ? Math.round(service.delaySeconds / service.seconds * 10) / 10 : null,
      observedMinutes: Math.round(service.seconds / 6) / 10,
      validIntervals: service.intervals, gainedDelayMinutes: service.gainedDelayMinutes, lastGpsAt: service.lastGpsAt,
    };
  }
  for (const service of services.values()) {
    validIntervals += service.intervals;
    const train = toTrain(service);
    const existing = latestServices.get(train.codComercial);
    if (service.row.captured_at >= sinceEpoch && train.lastGpsAt && (!existing || train.lastGpsAt > existing.lastGpsAt)) latestServices.set(train.codComercial, train);
  }
  const summaries = [...cells.values()].map(({ services, ...cell }) => ({ ...cell, delayedServices: [...services] }));
  return {
    sinceEpoch, untilEpoch, generatedAt: new Date(untilEpoch * 1000).toISOString(),
    validIntervals, rejectedSamples, repeatedSamples,
    trains: [...latestServices.values()], cells: summaries, zones: calculateMapDelayZones(summaries),
    method: { maxGapSeconds: MAX_GAP_SECONDS, maxSpeedKmh: MAX_SPEED_KMH, cellDegrees: CELL_DEGREES,
      minZoneIntervals: 3, minZoneTrains: 2, minDelayRate: 0.2 },
  };
};
