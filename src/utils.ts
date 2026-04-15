import type { NormalizedTrain, RawTrain } from "./types";

const toNullableString = (value: unknown): string | null => {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
};

const pickNullableString = (...values: unknown[]): string | null => {
  for (const value of values) {
    const normalized = toNullableString(value);
    if (normalized) {
      return normalized;
    }
  }

  return null;
};

const toNumber = (value: unknown): number | null => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  return null;
};

export const normalizeTrain = (input: RawTrain): NormalizedTrain | null => {
  const fallbackTrainCode = pickNullableString(input.codTren);
  const fallbackLine = pickNullableString(input.codLinea);
  const fallbackNucleo = pickNullableString(input.nucleo);
  const fallbackCommercialCode =
    fallbackTrainCode && (fallbackLine || fallbackNucleo)
      ? [fallbackTrainCode, fallbackLine, fallbackNucleo].filter(Boolean).join(":")
      : fallbackTrainCode;
  const codComercial =
    pickNullableString(input.codComercial, input.tripId) ?? fallbackCommercialCode;
  const latitud = toNumber(input.latitud);
  const longitud = toNumber(input.longitud);

  if (!codComercial || latitud === null || longitud === null) {
    return null;
  }

  const hasCommuterHints = Boolean(
    pickNullableString(input.codLinea, input.nucleo, input.codEstOrig, input.codEstDest),
  );
  const codProduct = toNumber(input.codProduct) ?? (hasCommuterHints ? 21 : -1);
  const delay = toNumber(input.ultRetraso) ?? toNumber(input.retrasoMin) ?? 0;

  return {
    codComercial,
    codEstAnt: pickNullableString(input.codEstAnt, input.codEstAct),
    codEstSig: pickNullableString(input.codEstSig, input.codEstAct),
    horaLlegadaSigEst: pickNullableString(input.horaLlegadaSigEst),
    codProduct,
    codOrigen: pickNullableString(input.codOrigen, input.codEstOrig),
    codDestino: pickNullableString(input.codDestino, input.codEstDest),
    desCorridor: pickNullableString(input.desCorridor),
    accesible: input.accesible ? 1 : 0,
    ultRetraso: Math.trunc(delay),
    latitud,
    longitud,
    gpsTime: toNumber(input.time),
    p: pickNullableString(input.p, input.via, input.nextVia),
    mat: pickNullableString(input.mat),
  };
};

export const computeSnapshotHash = (train: NormalizedTrain): string => {
  const raw = [
    train.codComercial,
    train.codEstAnt,
    train.codEstSig,
    train.horaLlegadaSigEst,
    train.codProduct,
    train.codOrigen,
    train.codDestino,
    train.desCorridor,
    train.accesible,
    train.ultRetraso,
    train.latitud.toFixed(5),
    train.longitud.toFixed(5),
    train.gpsTime,
    train.p,
    train.mat,
  ].join("|");

  return Bun.hash(raw).toString();
};

export const getDayString = (epochSeconds: number): string => {
  return new Date(epochSeconds * 1000).toISOString().slice(0, 10);
};

export const haversineKm = (
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number,
): number => {
  const toRadians = (degrees: number) => (degrees * Math.PI) / 180;
  const earthRadiusKm = 6371;

  const dLat = toRadians(lat2 - lat1);
  const dLon = toRadians(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRadians(lat1)) *
      Math.cos(toRadians(lat2)) *
      Math.sin(dLon / 2) ** 2;

  return 2 * earthRadiusKm * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
};

export const getDelayBucketFlags = (delay: number) => {
  return {
    ahead: delay < 0 ? 1 : 0,
    onTime: delay === 0 ? 1 : 0,
    mild: delay >= 1 && delay <= 15 ? 1 : 0,
    medium: delay >= 16 && delay <= 60 ? 1 : 0,
    severe: delay > 60 ? 1 : 0,
  };
};

export const toEpochSeconds = (): number => Math.floor(Date.now() / 1000);
