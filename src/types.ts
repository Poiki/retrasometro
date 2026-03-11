export interface RawTrain {
  codComercial?: string;
  codEstAnt?: string;
  codEstSig?: string;
  horaLlegadaSigEst?: string;
  codProduct?: number;
  codOrigen?: string;
  codDestino?: string;
  desCorridor?: string;
  accesible?: boolean;
  ultRetraso?: string | number;
  latitud?: number;
  longitud?: number;
  time?: number;
  p?: string;
  mat?: string;
}

export interface TrainsPayload {
  fechaActualizacion?: string;
  trenes?: RawTrain[];
}

export interface NormalizedTrain {
  codComercial: string;
  codEstAnt: string | null;
  codEstSig: string | null;
  horaLlegadaSigEst: string | null;
  codProduct: number;
  codOrigen: string | null;
  codDestino: string | null;
  desCorridor: string | null;
  accesible: number;
  ultRetraso: number;
  latitud: number;
  longitud: number;
  gpsTime: number | null;
  p: string | null;
  mat: string | null;
}

export interface StationRecord {
  code: string;
  name: string | null;
  locality: string | null;
  province: string | null;
  accessible: number | null;
  attended: number | null;
  correspondences: string | null;
  level: string | null;
  lat: number | null;
  lon: number | null;
}

export interface IngestionRun {
  fetchedAt: number;
  source: string;
  success: number;
  trainCount: number;
  skipped: number;
  error: string | null;
  providerUpdatedAt: string | null;
}

export interface CurrentTrainRow {
  cod_comercial: string;
  cod_product: number;
  cod_origen: string | null;
  cod_destino: string | null;
  cod_est_ant: string | null;
  cod_est_sig: string | null;
  hora_llegada_sig_est: string | null;
  des_corridor: string | null;
  accesible: number;
  ult_retraso: number;
  latitud: number;
  longitud: number;
  gps_time: number | null;
  p: string | null;
  mat: string | null;
  first_seen_at: number;
  last_seen_at: number;
  last_payload_hash: string | null;
  last_snapshot_at: number | null;
}

export interface DashboardOverview {
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

export interface ProductMetric {
  codProduct: number;
  productName: string;
  count: number;
  avgDelay: number;
  maxDelay: number;
}

export interface DelayBuckets {
  ahead: number;
  onTime: number;
  mild: number;
  medium: number;
  severe: number;
}
