import { initThemeController } from "./theme.js";
import "./vendor/leaflet-1.9.4/leaflet.js";
import "./vendor/maplibre/maplibre-gl.js";
import "./vendor/maplibre/leaflet-maplibre-gl.js";

const $ = (selector) => document.querySelector(selector);

const overviewCardsEl = $("#overview-cards");
const delayBucketsEl = $("#delay-buckets");
const productsEl = $("#products");
const corridorsEl = $("#corridors");
const todayMetricsEl = $("#today-metrics");
const trainsBodyEl = $("#trains-body");
const mapStatusEl = $("#map-status");
const mapTileErrorEl = $("#map-tile-error");
const mapFitBtnEl = $("#map-fit");
const mapTrainSelectEl = $("#map-train-select");
const mapZoomInEl = $("#map-zoom-in");
const mapZoomOutEl = $("#map-zoom-out");
const mapTrainDetailEl = $(".map-train-detail");
const mapDetailContentEl = $("#map-detail-content");
const mapLocationLinkEl = $("#map-location-link");
const corridorsSearchInputEl = $("#corridors-search");
const historicalProductsSearchInputEl = $("#historical-products-search");
const accountabilityRoutesSearchInputEl = $("#accountability-routes-search");
const accountabilityTrainsSearchInputEl = $("#accountability-trains-search");
const heroStatusEl = $("#hero-status");
const statusPillEl = $("#status-pill");
const liveTextEl = $("#live-text");
const footerStatusTextEl = $("#footer-status-text");
const lastSeenEl = $("#last-seen");
const historicalCardsEl = $("#historical-cards");
const historicalHighlightsEl = $("#historical-highlights");
const historicalProductsEl = $("#historical-products");
const accountabilityRoutesEl = $("#accountability-routes");
const accountabilityTrainsEl = $("#accountability-trains");
const trendChartEl = $("#trend-chart");
const historyButtons = [...document.querySelectorAll(".history-btn")];
const historyRangeEl = $("#history-range");
const historyRangeToggleEl = $("#history-range-toggle");
const historyRangeTextEl = $("#history-range-text");
const historyFromInputEl = $("#history-from");
const historyToInputEl = $("#history-to");
const historyRangeApplyBtnEl = $("#history-range-apply");
const historyRangeClearBtnEl = $("#history-range-clear");

const filtersForm = $("#filters");
const searchInput = $("#search");
const minDelayInput = $("#min-delay");
const syncBtnEl = $("#sync-btn");
const exportCsvBtnEl = $("#export-csv-btn");
const exportExcelBtnEl = $("#export-excel-btn");
const sortButtons = [...document.querySelectorAll("[data-sort]")];

const langSwitchEl = $("#lang-switch");
const themeSwitchEl = $("#theme-switch");
const apiDocsBtnEl = $("#api-docs-btn");

const I18N = {
  es: {
    title: "Retrasos ferroviarios en tiempo real",
    subtitle: "Monitoriza la puntualidad de los trenes en toda la red ferroviaria de España",
    navHome: "Inicio",
    navMap: "Mapa",
    navTrains: "Trenes",
    navStats: "Estadísticas",
    navApi: "API",
    navAbout: "Acerca de",
    langLabel: "Idioma",
    themeLabel: "Tema",
    themeSystem: "Sistema",
    themeLight: "Claro",
    themeDark: "Oscuro",
    apiDocs: "API",
    livePill: "Datos en tiempo real",
    liveError: "Sin datos recientes",
    delayTitle: "Distribución de retrasos",
    productsTitle: "Tipos de tren activos",
    corridorsTitle: "Corredores con más tráfico",
    todayTitle: "Acumulado del día",
    historicalTitle: "Estadísticas históricas",
    historicalHighlightsTitle: "Hallazgos del período",
    historicalProductsTitle: "Distribución histórica por tipo",
    accountabilityRoutesTitle: "Rutas reincidentes",
    accountabilityTrainsTitle: "Trenes reincidentes",
    trendTitle: "Evolución del retraso medio",
    trendAvg: "Retraso medio (min)",
    trendPeak: "Pico (min)",
    trendNoData: "Sin serie diaria para este período",
    historyFromLabel: "Desde",
    historyToLabel: "Hasta",
    historyApplyRange: "Aplicar rango",
    historyClearRange: "Limpiar",
    trainsTitle: "Trenes activos",
    mapTitle: "Mapa en tiempo real",
    mapDescription: "Últimas posiciones GPS publicadas por Renfe. Comparte los filtros de trenes y se actualiza con cada ingesta.",
    mapFit: "Ver la red",
    mapCount: "{shown} de {total} trenes con posición válida",
    mapEmpty: "No hay posiciones GPS válidas para los filtros actuales.",
    mapStale: "Sin datos recientes. Se conservan las últimas posiciones conocidas.",
    mapTileError: "No se pudo cargar el mapa base. Los trenes siguen disponibles; comprueba tu conexión.",
    mapZoomIn: "Acercar",
    mapZoomOut: "Alejar",
    mapGps: "Señal GPS",
    mapSelect: "Seleccionar tren",
    mapSelectPlaceholder: "Selecciona un tren",
    mapClose: "Cerrar ficha",
    mapLocate: "Centrar tren",
    mapOpenLocation: "OpenStreetMap",
    mapCoordinates: "Coordenadas publicadas",
    mapReceived: "Actualizado en el panel",
    mapViewTrain: "Ver tren {train} en el mapa",
    mapLatestSpeed: "Velocidad del último intervalo GPS",
    mapTrainAverageSpeed: "Velocidad media del servicio · 24 h",
    mapObserved: "Minutos con muestras válidas",
    mapGainedDelay: "Minutos de retraso añadidos",
    mapAverageDelay: "Retraso medio observado",
    mapNoSpeed: "Sin muestras fiables",
    mapZones: "Tramos con retrasos",
    mapZoneTitle: "Tramo con retrasos recurrentes",
    mapZoneTrains: "Servicios afectados",
    mapZoneHistory: "Histórico completo",
    mapZoneRecent: "Recientes · 24 h",
    mapZonePeriod: "Periodo de retrasos",
    mapZoneWorst: "Más problemáticos del histórico",
    mapZoneLatest: "Retrasos más recientes",
    mapZoneEmpty: "No hay tramos con retrasos recurrentes en este periodo.",
    mapZoneLast: "Último aumento de retraso",
    mapZonePeak: "Mayor retraso observado",
    mapZoneAverageAdded: "Retraso añadido por servicio afectado",
    mapZoneNear: "Cerca de {station}",
    mapZoneBackfill: "Preparando histórico: {done}/{total} días",
    mapAnalysisPending: "Preparando el análisis de retrasos…",
    mapAnalysisReady: "Análisis actualizado: {time}",
    mapZonesExplanation: "Las vías negras señalan tramos con aumentos repetidos de retraso. Cambia entre el histórico completo y las últimas 24 h; pulsa una vía para ver los servicios afectados y el retraso registrado.",
    apply: "Aplicar",
    searchPlaceholder: "Buscar por tren, corredor o estación...",
    minDelayLabel: "Min retraso",
    thTrain: "Tren",
    thType: "Tipo",
    thCorridor: "Corredor",
    thRoute: "Origen → Destino",
    thNext: "Prox. estación",
    thDelay: "Retraso",
    thLast: "Última señal",
    thRank: "#",
    thRouteShort: "Ruta",
    thObs: "Obs.",
    thAvg: "Retraso medio",
    thMax: "Máx.",
    thOver15: ">15 min",
    thOver60: ">60 min",
    headType: "Tipo",
    headObs: "Observaciones",
    cardActive: "Trenes activos",
    cardAvg: "Retraso medio",
    cardMax: "Retraso máximo",
    cardOver15: "> 15 min",
    cardOver60: "> 60 min",
    cardAccessible: "Accesibles",
    cardObs: "Observaciones hoy",
    cardKm: "Km trazados hoy",
    cardProblematicType: "Tren más problemático",
    cardVolumeType: "Tipo con más trenes/km",
    subActive: "en circulación ahora",
    subAvgToday: "ponderado hoy",
    subMaxToday: "pico de hoy",
    subOfTotal: "del total",
    subAccessible: "de {total} trenes",
    subObs: "registros procesados",
    subKm: "kilómetros recorridos",
    subProblematic: "minutos acumulados",
    subVolume: "trenes / km hoy",
    noTypeData: "Sin datos",
    histCardObservations: "Observaciones",
    histCardUniqueTrains: "Trenes únicos",
    histCardAvgDelay: "Retraso medio histórico",
    histCardMaxDelay: "Pico histórico",
    histCardAccumDelay: "Minutos acumulados",
    histCardOnTimePct: "Puntualidad",
    histCardSeverePct: "Retrasos severos",
    histCardAvgBatch: "Media trenes por ciclo",
    histTopType: "Tipo más problemático",
    histTopCorridor: "Corredor más problemático",
    histNoCorridor: "Sin corredor",
    histNoData: "Sin datos históricos para este período",
    accountabilityNoData: "Sin datos de reincidencia para este período",
    histRangeApplied: "Rango histórico aplicado",
    histRangeCleared: "Rango histórico limpiado",
    histObsShort: "obs",
    histTrainsShort: "trenes",
    accountabilityDelayed15: "% >15 min",
    accountabilitySevere60: "% >60 min",
    accountabilityTrendLabel: "Tendencia 24h vs 7d",
    accountabilityTrendWorsening: "Empeora",
    accountabilityTrendImproving: "Mejora",
    accountabilityTrendFlat: "Estable",
    bucketAhead: "Adelantados",
    bucketOnTime: "En hora",
    bucketMild: "1-15 min",
    bucketMedium: "16-60 min",
    bucketSevere: "> 60 min",
    corridorActive: "trenes activos",
    corridorDelay: "Retraso medio",
    corridorPeak: "pico",
    corridorSourceOfficial: "oficial",
    corridorSourceDerived: "eje derivado",
    todayDate: "Fecha",
    todayUnique: "Trenes únicos",
    todayObs: "Observaciones",
    todayWeighted: "Retraso medio ponderado",
    todayPeak: "Pico del día",
    todayKm: "Km estimados",
    statusLoading: "Cargando datos...",
    statusLastUpdate: "Última actualización",
    statusNoUpdate: "Sin actualización",
    statusError: "Error",
    lastSignal: "Última señal",
    noRows: "No hay trenes para este filtro.",
    noLocalRows: "No hay resultados para el filtro local.",
    corridorsSearchPlaceholder: "Filtrar corredores",
    historicalProductsSearchPlaceholder: "Filtrar tipos",
    accountabilityRoutesSearchPlaceholder: "Filtrar rutas críticas",
    accountabilityTrainsSearchPlaceholder: "Filtrar trenes reincidentes",
    tooltipInfoAria: "Más información de la métrica",
    tipAccumDelay:
      "Suma de minutos con retraso positivo en el período. Fórmula: Σ max(retraso,0). Cuanto mayor, peor impacto acumulado.",
    tipWeightedAvgDelay:
      "Retraso medio ponderado por observación. Fórmula: Σ retraso / N observaciones. Resume el retraso típico real.",
    tipDelayed15:
      "Porcentaje de observaciones con retraso mayor de 15 min. Fórmula: obs(retraso>15)/obs totales.",
    tipSevere60:
      "Porcentaje de observaciones con retraso mayor de 60 min. Fórmula: obs(retraso>60)/obs totales.",
    tipRepeatOffenders:
      "Trenes que repiten retrasos altos en el período. Se ordenan por %>15, luego %>60 y volumen de observaciones.",
    tipCriticalRoutes:
      "Ejes origen↔destino con peor reincidencia. Se agrupan A↔B juntos y se exige un mínimo de observaciones.",
    tipAvgTrainsPerBatch:
      "Media de trenes recibidos por ciclo de ingesta. Ayuda a detectar bajadas de cobertura del proveedor.",
    routeSeparator: " → ",
    keyRequestFailed: "No se pudo obtener clave API",
    syncNow: "Sincronizar",
    exportCsv: "Exportar CSV",
    exportExcel: "Exportar Excel",
    exportDone: "Exportación completada",
    exportFail: "No se pudo exportar",
    minutes: "min",
    docsOpenFail: "No se pudo abrir la documentación API",
    footerTagline: "Datos abiertos para un transporte más puntual",
    footerApi: "API",
    footerDocs: "Documentación",
    footerMethod: "Metodología",
    footerSource: "Código",
  },
  en: {
    title: "Real-time rail delays",
    subtitle: "Track train punctuality across the whole Spanish rail network",
    navHome: "Home",
    navMap: "Map",
    navTrains: "Trains",
    navStats: "Statistics",
    navApi: "API",
    navAbout: "About",
    langLabel: "Language",
    themeLabel: "Theme",
    themeSystem: "System",
    themeLight: "Light",
    themeDark: "Dark",
    apiDocs: "API",
    livePill: "Live data",
    liveError: "No recent data",
    delayTitle: "Delay distribution",
    productsTitle: "Active train types",
    corridorsTitle: "Top traffic corridors",
    todayTitle: "Today aggregate",
    historicalTitle: "Historical statistics",
    historicalHighlightsTitle: "Period highlights",
    historicalProductsTitle: "Historical distribution by type",
    accountabilityRoutesTitle: "Repeat routes",
    accountabilityTrainsTitle: "Repeat trains",
    trendTitle: "Average delay over time",
    trendAvg: "Average delay (min)",
    trendPeak: "Peak (min)",
    trendNoData: "No daily series for this range",
    historyFromLabel: "From",
    historyToLabel: "To",
    historyApplyRange: "Apply range",
    historyClearRange: "Clear",
    trainsTitle: "Active trains",
    mapTitle: "Live map",
    mapDescription: "Latest GPS positions published by Renfe. Uses the train filters and updates after each ingestion.",
    mapFit: "Show network",
    mapCount: "{shown} of {total} trains with valid positions",
    mapEmpty: "No valid GPS positions match the current filters.",
    mapStale: "No recent data. Showing the last known positions.",
    mapTileError: "Could not load the base map. Trains are still available; check your connection.",
    mapZoomIn: "Zoom in",
    mapZoomOut: "Zoom out",
    mapGps: "GPS signal",
    mapSelect: "Select train",
    mapSelectPlaceholder: "Choose a train",
    mapClose: "Close card",
    mapLocate: "Center train",
    mapOpenLocation: "OpenStreetMap",
    mapCoordinates: "Published coordinates",
    mapReceived: "Updated in the dashboard",
    mapViewTrain: "Show train {train} on the map",
    mapLatestSpeed: "Speed over the latest GPS interval",
    mapTrainAverageSpeed: "Service average speed · 24 h",
    mapObserved: "Minutes with valid samples",
    mapGainedDelay: "Added delay minutes",
    mapAverageDelay: "Observed average delay",
    mapNoSpeed: "No reliable samples",
    mapZones: "Sections with delays",
    mapZoneTitle: "Recurring delay section",
    mapZoneTrains: "Affected services",
    mapZoneHistory: "Full history",
    mapZoneRecent: "Recent · 24 h",
    mapZonePeriod: "Delay period",
    mapZoneWorst: "Most problematic in the full history",
    mapZoneLatest: "Most recent delays",
    mapZoneEmpty: "No sections with recurring delays in this period.",
    mapZoneLast: "Latest increase in delay",
    mapZonePeak: "Largest observed delay",
    mapZoneAverageAdded: "Added delay per affected service",
    mapZoneNear: "Near {station}",
    mapZoneBackfill: "Preparing history: {done}/{total} days",
    mapAnalysisPending: "Preparing delay analysis…",
    mapAnalysisReady: "Analysis updated: {time}",
    mapZonesExplanation: "Black tracks mark sections with repeated increases in delay. Switch between the full history and the last 24 hours; select a track to see affected services and recorded delays.",
    apply: "Apply",
    searchPlaceholder: "Search by train, corridor or station...",
    minDelayLabel: "Min delay",
    thTrain: "Train",
    thType: "Type",
    thCorridor: "Corridor",
    thRoute: "Origin → Destination",
    thNext: "Next station",
    thDelay: "Delay",
    thLast: "Last signal",
    thRank: "#",
    thRouteShort: "Route",
    thObs: "Obs.",
    thAvg: "Avg delay",
    thMax: "Max",
    thOver15: ">15 min",
    thOver60: ">60 min",
    headType: "Type",
    headObs: "Observations",
    cardActive: "Active trains",
    cardAvg: "Average delay",
    cardMax: "Max delay",
    cardOver15: "> 15 min",
    cardOver60: "> 60 min",
    cardAccessible: "Accessible",
    cardObs: "Observations today",
    cardKm: "Tracked km today",
    cardProblematicType: "Most problematic type",
    cardVolumeType: "Top type by trains/km",
    subActive: "running right now",
    subAvgToday: "weighted today",
    subMaxToday: "today's peak",
    subOfTotal: "of total",
    subAccessible: "of {total} trains",
    subObs: "records processed",
    subKm: "kilometres tracked",
    subProblematic: "accumulated minutes",
    subVolume: "trains / km today",
    noTypeData: "No data",
    histCardObservations: "Observations",
    histCardUniqueTrains: "Unique trains",
    histCardAvgDelay: "Historical average delay",
    histCardMaxDelay: "Historical peak",
    histCardAccumDelay: "Accumulated minutes",
    histCardOnTimePct: "Punctuality",
    histCardSeverePct: "Severe delays",
    histCardAvgBatch: "Avg trains per run",
    histTopType: "Most problematic type",
    histTopCorridor: "Most problematic corridor",
    histNoCorridor: "No corridor",
    histNoData: "No historical data for this range",
    accountabilityNoData: "No repeat-offender data for this range",
    histRangeApplied: "Historical range applied",
    histRangeCleared: "Historical range cleared",
    histObsShort: "obs",
    histTrainsShort: "trains",
    accountabilityDelayed15: "% >15 min",
    accountabilitySevere60: "% >60 min",
    accountabilityTrendLabel: "24h vs 7d trend",
    accountabilityTrendWorsening: "Worsening",
    accountabilityTrendImproving: "Improving",
    accountabilityTrendFlat: "Stable",
    bucketAhead: "Ahead",
    bucketOnTime: "On time",
    bucketMild: "1-15 min",
    bucketMedium: "16-60 min",
    bucketSevere: "> 60 min",
    corridorActive: "active trains",
    corridorDelay: "Average delay",
    corridorPeak: "peak",
    corridorSourceOfficial: "official",
    corridorSourceDerived: "derived axis",
    todayDate: "Date",
    todayUnique: "Unique trains",
    todayObs: "Observations",
    todayWeighted: "Weighted average delay",
    todayPeak: "Peak of day",
    todayKm: "Estimated km",
    statusLoading: "Loading data...",
    statusLastUpdate: "Last update",
    statusNoUpdate: "No updates yet",
    statusError: "Error",
    lastSignal: "Last signal",
    noRows: "No trains match the current filters.",
    noLocalRows: "No results for this local filter.",
    corridorsSearchPlaceholder: "Filter corridors",
    historicalProductsSearchPlaceholder: "Filter types",
    accountabilityRoutesSearchPlaceholder: "Filter critical routes",
    accountabilityTrainsSearchPlaceholder: "Filter repeat trains",
    tooltipInfoAria: "More metric info",
    tipAccumDelay:
      "Sum of positive delay minutes during the window. Formula: Σ max(delay,0). Higher means worse accumulated impact.",
    tipWeightedAvgDelay:
      "Observation-weighted average delay. Formula: Σ delay / N observations. Captures typical real delay.",
    tipDelayed15:
      "Percentage of observations delayed more than 15 min. Formula: obs(delay>15)/total observations.",
    tipSevere60:
      "Percentage of observations delayed more than 60 min. Formula: obs(delay>60)/total observations.",
    tipRepeatOffenders:
      "Trains repeatedly showing high delays in the period. Sorted by %>15, then %>60, then observation volume.",
    tipCriticalRoutes:
      "Origin↔destination axes with worst repeat delay ratios. A↔B and B↔A are grouped, with minimum observation thresholds.",
    tipAvgTrainsPerBatch:
      "Average trains per ingestion batch. Useful to detect provider coverage drops.",
    routeSeparator: " → ",
    keyRequestFailed: "Could not request API key",
    syncNow: "Sync",
    exportCsv: "Export CSV",
    exportExcel: "Export Excel",
    exportDone: "Export completed",
    exportFail: "Could not export",
    minutes: "min",
    docsOpenFail: "Could not open API docs",
    footerTagline: "Open data for a more punctual railway",
    footerApi: "API",
    footerDocs: "Documentation",
    footerMethod: "Methodology",
    footerSource: "Source code",
  },
};

const state = {
  query: "",
  minDelay: "",
  limit: 120,
  offset: 0,
  lang: localStorage.getItem("retrasometro_lang") === "en" ? "en" : "es",
  historyHours: Number(localStorage.getItem("retrasometro_history_hours")) || 168,
  historyFrom: "",
  historyTo: "",
  corridorsQuery: "",
  historicalProductsQuery: "",
  accountabilityRoutesQuery: "",
  accountabilityTrainsQuery: "",
  apiKey: null,
  apiKeyExpiresAt: 0,
  apiMinIntervalMs: 200,
  requestQueue: Promise.resolve(),
  lastRequestAtMs: 0,
  lastAppliedSuccessAt: 0,
  hasPendingSync: false,
  trainsItems: [],
  sortBy: "delay",
  sortDir: "desc",
};

const AUTO_REFRESH_MS = 5_000;

let latestDashboard = null;
let numberFmt = new Intl.NumberFormat("es-ES");

const t = (key) => I18N[state.lang][key] || key;
const locale = () => (state.lang === "es" ? "es-ES" : "en-US");
const icon = (name, extraClass = "") => `<svg class="${extraClass}"><use href="#i-${name}" /></svg>`;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const asTime = (epochSeconds) => {
  if (!epochSeconds) {
    return "-";
  }

  return new Date(epochSeconds * 1000).toLocaleString(locale(), {
    hour12: false,
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
};

const asDate = (epochSeconds) =>
  epochSeconds
    ? new Date(epochSeconds * 1000).toLocaleDateString(locale(), { day: "2-digit", month: "2-digit", year: "numeric" })
    : "-";

const escapeHtml = (value) => {
  if (value === null || value === undefined) {
    return "";
  }

  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
};

const metricLabelWithInfo = (label, infoText, idSuffix) => {
  const tipId = `tip-${idSuffix}`;
  return `<span class="label-with-tip">${escapeHtml(label)}<span class="metric-info"><button type="button" class="info-trigger" aria-label="${escapeHtml(t("tooltipInfoAria"))}" aria-controls="${tipId}" aria-expanded="false">${icon("info")}</button><span id="${tipId}" class="info-popover" role="tooltip">${escapeHtml(infoText)}</span></span></span>`;
};

const closeOpenTooltips = () => {
  for (const wrapper of document.querySelectorAll(".metric-info.open")) {
    wrapper.classList.remove("open");
    wrapper.querySelector(".info-trigger")?.setAttribute("aria-expanded", "false");
  }
};

const toBigIntLoose = (value) => {
  if (typeof value === "bigint") {
    return value;
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? BigInt(Math.trunc(value)) : 0n;
  }

  if (typeof value === "string") {
    const intLike = value.trim().match(/^(-?\d+)(?:[.,]\d+)?$/);
    if (intLike) {
      try {
        return BigInt(intLike[1]);
      } catch {
        return 0n;
      }
    }
  }

  return 0n;
};

const formatLargeInt = (value) => {
  const parsed = toBigIntLoose(value);
  try {
    return numberFmt.format(parsed);
  } catch {
    return parsed.toString();
  }
};

const fmt1 = (value) => Number(value || 0).toFixed(1);
const pct = (part, total) => (total > 0 ? ((part / total) * 100).toFixed(1) : "0.0");

const delayClass = (delay) => {
  if (delay < 0) return "ahead";
  if (delay === 0) return "ok";
  if (delay <= 15) return "mild";
  if (delay <= 60) return "medium";
  return "severe";
};

const delayTone = (delay) => (delay > 60 ? "tone-danger" : delay > 15 ? "tone-warning" : "");

const delayLabel = (delay) => `${delay > 0 ? "+" : ""}${delay} ${t("minutes")}`;

const setStatus = (text, type = "") => {
  statusPillEl.textContent = text;
  footerStatusTextEl.textContent = text;
  heroStatusEl.classList.remove("ok", "error");
  if (type) {
    heroStatusEl.classList.add(type);
  }
  liveTextEl.textContent = type === "error" ? t("liveError") : t("livePill");
};

const updateTextContent = (id, value) => {
  const element = document.getElementById(id);
  if (element) {
    element.textContent = value;
  }
};

const setPendingSync = (pending) => {
  state.hasPendingSync = pending;
  syncBtnEl.classList.toggle("hidden", !pending);
};

const normalizeText = (value) => String(value ?? "").toLocaleLowerCase(locale());

const stationPair = (origin, destination, originCode, destinationCode) =>
  `${origin || originCode || "-"}${t("routeSeparator")}${destination || destinationCode || "-"}`;

const nextStation = (nextName, nextCode, eta) => {
  const station = nextName || nextCode || "-";
  return eta ? `${station} (${eta.replace("T", " ").slice(0, 16)})` : station;
};

const sourceBadge = (source) =>
  source === "derived_axis"
    ? `<span class="source-badge derived">${escapeHtml(t("corridorSourceDerived"))}</span>`
    : `<span class="source-badge official">${escapeHtml(t("corridorSourceOfficial"))}</span>`;

const getTrainSortValue = (train, field) => {
  if (field === "train") return normalizeText(train.cod_comercial);
  if (field === "type") return normalizeText(train.product_name);
  if (field === "corridor") return normalizeText(train.des_corridor);
  if (field === "route")
    return normalizeText(stationPair(train.origin_name, train.destination_name, train.cod_origen, train.cod_destino));
  if (field === "next") return normalizeText(nextStation(train.next_station_name, train.cod_est_sig, train.hora_llegada_sig_est));
  if (field === "delay") return Number(train.ult_retraso ?? 0);
  if (field === "last") return Number(train.last_seen_at ?? 0);
  return "";
};

const sortTrains = (items) => {
  const direction = state.sortDir === "asc" ? 1 : -1;
  return [...items].sort((a, b) => {
    const va = getTrainSortValue(a, state.sortBy);
    const vb = getTrainSortValue(b, state.sortBy);
    if (typeof va === "number" && typeof vb === "number") {
      return (va - vb) * direction;
    }
    return String(va).localeCompare(String(vb), locale()) * direction;
  });
};

const renderSortIndicators = () => {
  for (const button of sortButtons) {
    const field = button.dataset.sort;
    const isActive = field === state.sortBy;
    button.classList.toggle("active", isActive);
    updateTextContent(`sort-indicator-${field}`, isActive ? (state.sortDir === "asc" ? "↑" : "↓") : "↕");
  }
};

const setHistoryButtonsActive = () => {
  const hasCustom = Boolean(state.historyFrom || state.historyTo);
  for (const button of historyButtons) {
    button.classList.toggle("active", !hasCustom && Number(button.dataset.hours) === state.historyHours);
  }
};

const renderHistoryRange = (historical) => {
  if (!historical) {
    historyRangeTextEl.textContent = "…";
    return;
  }
  historyRangeTextEl.textContent = `${asDate(historical.sinceEpoch)} → ${asDate(historical.untilEpoch ?? historical.nowEpoch)}`;
};

const applyStaticTexts = () => {
  document.documentElement.lang = state.lang;
  numberFmt = new Intl.NumberFormat(locale());

  const plain = {
    "title-text": "title",
    "subtitle-text": "subtitle",
    "nav-home": "navHome",
    "nav-map": "navMap",
    "nav-trains": "navTrains",
    "nav-stats": "navStats",
    "nav-api": "navApi",
    "nav-about": "navAbout",
    "lang-label": "langLabel",
    "theme-label": "themeLabel",
    "theme-option-system": "themeSystem",
    "theme-option-light": "themeLight",
    "theme-option-dark": "themeDark",
    "api-docs-label": "apiDocs",
    "delay-title": "delayTitle",
    "products-title": "productsTitle",
    "corridors-title": "corridorsTitle",
    "today-title": "todayTitle",
    "historical-title": "historicalTitle",
    "historical-highlights-title": "historicalHighlightsTitle",
    "historical-products-title": "historicalProductsTitle",
    "trend-title": "trendTitle",
    "trend-legend-avg": "trendAvg",
    "trend-legend-peak": "trendPeak",
    "hp-head-type": "headType",
    "hp-head-obs": "headObs",
    "hp-head-avg": "thAvg",
    "history-from-label": "historyFromLabel",
    "history-to-label": "historyToLabel",
    "history-range-apply": "historyApplyRange",
    "history-range-clear": "historyClearRange",
    "trains-title": "trainsTitle",
    "map-title": "mapTitle",
    "map-description": "mapDescription",
    "map-fit": "mapFit",
    "map-select-label": "mapSelect",
    "map-locate-train": "mapLocate",
    "map-location-link": "mapOpenLocation",
    "map-zones-label": "mapZones",
    "map-zones-description": "mapZonesExplanation",
    "map-zones-toggle-label": "mapZones",
    "map-zone-history": "mapZoneHistory",
    "map-zone-recent": "mapZoneRecent",
    "map-tile-error": "mapTileError",
    "min-delay-label": "minDelayLabel",
    "min-delay-unit": "minutes",
    "apply-btn": "apply",
    "sync-btn": "syncNow",
    "export-csv-label": "exportCsv",
    "export-excel-label": "exportExcel",
    "th-train": "thTrain",
    "th-type": "thType",
    "th-corridor": "thCorridor",
    "th-route": "thRoute",
    "th-next": "thNext",
    "th-delay": "thDelay",
    "th-last": "thLast",
    "footer-tagline": "footerTagline",
    "footer-api": "footerApi",
    "footer-docs": "footerDocs",
    "footer-method": "footerMethod",
    "footer-source": "footerSource",
  };
  for (const [id, key] of Object.entries(plain)) {
    updateTextContent(id, t(key));
  }

  $("#accountability-routes-title").innerHTML = metricLabelWithInfo(
    t("accountabilityRoutesTitle"),
    t("tipCriticalRoutes"),
    "title-critical-routes",
  );
  $("#accountability-trains-title").innerHTML = metricLabelWithInfo(
    t("accountabilityTrainsTitle"),
    t("tipRepeatOffenders"),
    "title-repeat-offenders",
  );

  searchInput.placeholder = t("searchPlaceholder");
  corridorsSearchInputEl.placeholder = t("corridorsSearchPlaceholder");
  historicalProductsSearchInputEl.placeholder = t("historicalProductsSearchPlaceholder");
  accountabilityRoutesSearchInputEl.placeholder = t("accountabilityRoutesSearchPlaceholder");
  accountabilityTrainsSearchInputEl.placeholder = t("accountabilityTrainsSearchPlaceholder");
  $(".map-legend").setAttribute("aria-label", t("thDelay"));
  for (const [control, key] of [[mapZoomInEl, "mapZoomIn"], [mapZoomOutEl, "mapZoomOut"], [$("#map-detail-close"), "mapClose"]]) {
    control.title = t(key);
    control.setAttribute("aria-label", t(key));
  }

  if (!latestDashboard) {
    setStatus(t("statusLoading"));
  } else {
    liveTextEl.textContent = heroStatusEl.classList.contains("error") ? t("liveError") : t("livePill");
  }

  setHistoryButtonsActive();
  historyFromInputEl.value = state.historyFrom;
  historyToInputEl.value = state.historyTo;
  renderSortIndicators();
};

const buildUrl = (path) => new URL(path, window.location.origin).toString();

const requestApiKey = async ({ force = false } = {}) => {
  if (!force && state.apiKey && Date.now() < state.apiKeyExpiresAt - 10_000) {
    return;
  }

  const response = await fetch(buildUrl("/api/auth/request-key"), {
    method: "GET",
    cache: "no-store",
    headers: { "accept-language": state.lang },
  });

  if (!response.ok) {
    throw new Error(`${t("keyRequestFailed")} (${response.status})`);
  }

  const data = await response.json();
  state.apiKey = data.apiKey;
  state.apiKeyExpiresAt = Number(data.expiresAt || 0);
  const minInterval = Number(data?.limits?.minIntervalMs);
  if (Number.isFinite(minInterval) && minInterval >= 0) {
    state.apiMinIntervalMs = Math.max(0, Math.trunc(minInterval));
  }
};

const isAuthError = (payload) =>
  Boolean(payload) &&
  typeof payload === "object" &&
  ["MISSING_API_KEY", "INVALID_API_KEY", "EXPIRED_API_KEY"].includes(payload.code);

const apiFetch = async (path, options = {}) => {
  const execute = async () => {
    await requestApiKey();

    for (let attempt = 0; attempt < 3; attempt += 1) {
      const waitMs = Math.max(0, state.apiMinIntervalMs + 10 - (Date.now() - state.lastRequestAtMs));
      if (waitMs > 0) {
        await sleep(waitMs);
      }

      const headers = new Headers(options.headers || {});
      headers.set("accept-language", state.lang);
      headers.set("x-api-key", state.apiKey || "");

      state.lastRequestAtMs = Date.now();
      const response = await fetch(buildUrl(path), { ...options, headers, cache: "no-store" });

      if (response.status === 401) {
        let payload = null;
        try {
          payload = await response.clone().json();
        } catch {
          payload = null;
        }

        if (isAuthError(payload) && attempt < 2) {
          await requestApiKey({ force: true });
          continue;
        }
      }

      if (response.status === 429 && attempt < 2) {
        await sleep(state.apiMinIntervalMs + 10);
        continue;
      }

      return response;
    }

    throw new Error("Unexpected API flow");
  };

  const queued = state.requestQueue.then(execute, execute);
  state.requestQueue = queued.then(
    () => undefined,
    () => undefined,
  );

  return queued;
};

const readJsonResponse = async (response) => {
  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }

  if (!response.ok) {
    throw new Error(payload?.message || `${response.status}`);
  }

  return payload;
};

const cardHtml = ({ icon: iconName, iconTone = "", tone = "", label, labelHtml, value, sub, subClass = "", progress }) => `
  <article class="card ${tone}">
    <div class="kpi-icon ${iconTone}">${icon(iconName)}</div>
    <div class="kpi-body">
      <div class="label">${labelHtml ?? escapeHtml(label)}</div>
      <div class="value ${/^\d/.test(String(value)) ? "" : "text"}">${escapeHtml(value)}</div>
      ${progress !== undefined ? `<div class="progress"><i style="width:${Math.max(0, Math.min(100, progress)).toFixed(1)}%"></i></div>` : ""}
      ${sub ? `<div class="sub ${subClass}">${escapeHtml(sub)}</div>` : ""}
    </div>
  </article>
`;

const renderOverviewCards = (overview, today, typeInsights) => {
  const problematic = typeInsights?.problematic ?? null;
  const volume = typeInsights?.volume ?? null;
  const active = Number(overview.activeTrains || 0);

  const cards = [
    { icon: "train", label: t("cardActive"), value: numberFmt.format(active), sub: t("subActive") },
    {
      icon: "clock",
      tone: delayTone(overview.avgDelay),
      label: t("cardAvg"),
      value: `${fmt1(overview.avgDelay)} ${t("minutes")}`,
      sub: `${t("subAvgToday")}: ${fmt1(today.weightedAvgDelay)} ${t("minutes")}`,
    },
    {
      icon: "alert",
      tone: delayTone(overview.maxDelay),
      label: t("cardMax"),
      value: `${overview.maxDelay} ${t("minutes")}`,
      sub: `${t("subMaxToday")}: ${today.peakDelay} ${t("minutes")}`,
    },
    {
      icon: "pie",
      iconTone: overview.delayedOver15 > 0 ? "warning" : "",
      label: t("cardOver15"),
      value: numberFmt.format(overview.delayedOver15),
      sub: `${pct(overview.delayedOver15, active)}% ${t("subOfTotal")}`,
    },
    {
      icon: "alert-circle",
      iconTone: overview.severeOver60 > 0 ? "danger" : "",
      label: t("cardOver60"),
      value: numberFmt.format(overview.severeOver60),
      sub: `${pct(overview.severeOver60, active)}% ${t("subOfTotal")}`,
    },
    {
      icon: "access",
      label: t("cardAccessible"),
      value: `${overview.accessiblePct}%`,
      progress: Number(overview.accessiblePct || 0),
      sub: `${numberFmt.format(overview.accessibleCount)} ${t("subAccessible").replace("{total}", numberFmt.format(active))}`,
    },
    { icon: "database", label: t("cardObs"), value: numberFmt.format(today.observations), sub: t("subObs") },
    { icon: "route", label: t("cardKm"), value: numberFmt.format(Math.round(today.kmTracked)), sub: t("subKm") },
    {
      icon: "alert",
      iconTone: "danger",
      label: t("cardProblematicType"),
      value: problematic ? problematic.productName : t("noTypeData"),
      sub: problematic ? `${formatLargeInt(problematic.accumulatedDelayMinutes)} ${t("subProblematic")}` : "",
      subClass: "danger",
    },
    {
      icon: "bar",
      label: t("cardVolumeType"),
      value: volume ? volume.productName : t("noTypeData"),
      sub: volume ? `${numberFmt.format(volume.trains)} / ${numberFmt.format(Math.round(volume.totalKm))} km · ${t("subVolume")}` : "",
      subClass: "accent",
    },
  ];

  overviewCardsEl.innerHTML = cards.map(cardHtml).join("");
};

const renderBarRows = (target, rows) => {
  target.classList.remove("skeleton-bars");
  target.innerHTML = rows
    .map((row) => {
      const width = Math.max(0, Math.min(100, row.pct)).toFixed(1);
      return `
        <div class="bar-row">
          <span title="${escapeHtml(row.label)}">${escapeHtml(row.label)}</span>
          <div class="track"><div class="fill ${row.tone ?? ""}" style="width: ${width}%"></div></div>
          <strong>${escapeHtml(row.value)}</strong>
          ${row.extra !== undefined ? `<em>${escapeHtml(row.extra)}</em>` : ""}
        </div>
      `;
    })
    .join("");
};

const renderDelayBuckets = (buckets) => {
  const total = Object.values(buckets).reduce((acc, val) => acc + val, 0) || 1;
  const rows = [
    { label: t("bucketAhead"), value: buckets.ahead, tone: "muted" },
    { label: t("bucketOnTime"), value: buckets.onTime, tone: "ok" },
    { label: t("bucketMild"), value: buckets.mild, tone: "ok" },
    { label: t("bucketMedium"), value: buckets.medium, tone: "warning" },
    { label: t("bucketSevere"), value: buckets.severe, tone: "danger" },
  ].map((row) => ({ ...row, pct: (row.value / total) * 100, value: numberFmt.format(row.value) }));

  renderBarRows(delayBucketsEl, rows);
};

const toneForAvg = (avg) => (avg > 60 ? "danger" : avg > 15 ? "warning" : "");

const renderProducts = (products) => {
  const max = products.reduce((acc, item) => Math.max(acc, item.count), 1);
  const rows = products.slice(0, 10).map((item) => ({
    label: `${item.productName} (${item.codProduct})`,
    value: `${item.count} · ${fmt1(item.avgDelay)} ${t("minutes")}`,
    pct: (item.count / max) * 100,
    tone: toneForAvg(item.avgDelay),
  }));

  renderBarRows(productsEl, rows);
};

const emptyRow = (text) => `<div class="list-item"><div class="meta">${escapeHtml(text)}</div></div>`;

const matchesFilter = (filter, ...fields) =>
  filter.length === 0 || fields.map((f) => String(f || "")).join(" ").toLocaleLowerCase(locale()).includes(filter);

const renderCorridors = (corridors) => {
  corridorsEl.classList.remove("skeleton-list");
  const filter = normalizeText(state.corridorsQuery.trim());
  const visible = corridors.filter((item) => matchesFilter(filter, item.corridor, item.axisLabel, item.axisKey));

  if (visible.length === 0) {
    corridorsEl.innerHTML = emptyRow(t("noLocalRows"));
    return;
  }

  corridorsEl.innerHTML = visible
    .map(
      (item) => `
      <div class="list-item">
        <div class="name">${escapeHtml(item.corridor)} ${sourceBadge(item.source)}</div>
        <div class="meta">${numberFmt.format(item.train_count)} ${t("corridorActive")} · ${t("corridorDelay")}: ${fmt1(item.avg_delay)} ${t("minutes")} · ${t("corridorPeak")}: ${item.max_delay} ${t("minutes")}</div>
      </div>
    `,
    )
    .join("");
};

const renderToday = (today) => {
  todayMetricsEl.classList.remove("skeleton-today");
  const items = [
    { label: t("todayDate"), value: today.day },
    { label: t("todayUnique"), value: numberFmt.format(today.uniqueTrains) },
    { label: t("todayObs"), value: numberFmt.format(today.observations) },
    {
      labelHtml: metricLabelWithInfo(t("todayWeighted"), t("tipWeightedAvgDelay"), "today-weighted-delay"),
      value: `${fmt1(today.weightedAvgDelay)} ${t("minutes")}`,
    },
    { label: t("todayPeak"), value: `${today.peakDelay} ${t("minutes")}` },
    { label: t("todayKm"), value: `${numberFmt.format(Math.round(today.kmTracked))} km` },
  ];

  todayMetricsEl.innerHTML = items
    .map(
      (item) => `
      <div class="today-box">
        <div class="label">${item.labelHtml ?? escapeHtml(item.label)}</div>
        <div class="value">${escapeHtml(item.value)}</div>
      </div>
    `,
    )
    .join("");
};

const pctCell = (value) => {
  const n = Number(value || 0);
  return `<td class="num ${n >= 50 ? "bad" : n >= 20 ? "warn" : ""}">${n.toFixed(1)}%</td>`;
};

const miniTable = (headers, rows) => `
  <table class="mini-table">
    <thead><tr>${headers.map((h) => `<th class="${h.num ? "num" : ""}">${escapeHtml(h.label)}</th>`).join("")}</tr></thead>
    <tbody>${rows.join("")}</tbody>
  </table>
`;

const renderAccountability = (accountability) => {
  accountabilityRoutesEl.classList.remove("skeleton-list");
  accountabilityTrainsEl.classList.remove("skeleton-list");

  if (!accountability || !accountability.summary) {
    accountabilityRoutesEl.innerHTML = emptyRow(t("accountabilityNoData"));
    accountabilityTrainsEl.innerHTML = emptyRow(t("accountabilityNoData"));
    return;
  }

  const routesFilter = normalizeText(state.accountabilityRoutesQuery.trim());
  const routes = (Array.isArray(accountability.criticalRoutes) ? accountability.criticalRoutes : [])
    .filter((item) => matchesFilter(routesFilter, item.axisLabel, item.axisKey))
    .slice(0, 12);

  accountabilityRoutesEl.innerHTML =
    routes.length === 0
      ? emptyRow(t("noLocalRows"))
      : miniTable(
          [
            { label: t("thRank") },
            { label: t("thRouteShort") },
            { label: t("thObs"), num: true },
            { label: t("thAvg"), num: true },
            { label: t("thMax"), num: true },
            { label: t("thOver15"), num: true },
            { label: t("thOver60"), num: true },
          ],
          routes.map(
            (item, index) => `
            <tr>
              <td class="rank">${index + 1}</td>
              <td class="route">${escapeHtml(item.axisLabel || "-")} ${sourceBadge(item.source)}</td>
              <td class="num">${numberFmt.format(item.observations || 0)}</td>
              <td class="num">${fmt1(item.avgDelay)}</td>
              <td class="num">${item.maxDelay || 0}</td>
              ${pctCell(item.delayed15Pct)}
              ${pctCell(item.severe60Pct)}
            </tr>
          `,
          ),
        );

  const offendersFilter = normalizeText(state.accountabilityTrainsQuery.trim());
  const offenders = (Array.isArray(accountability.repeatOffenders) ? accountability.repeatOffenders : [])
    .filter((item) =>
      matchesFilter(offendersFilter, item.codComercial, item.productName || item.codProduct, item.origin, item.destination),
    )
    .slice(0, 12);

  accountabilityTrainsEl.innerHTML =
    offenders.length === 0
      ? emptyRow(t("noLocalRows"))
      : miniTable(
          [
            { label: t("thRank") },
            { label: t("thTrain") },
            { label: t("thRouteShort") },
            { label: t("thObs"), num: true },
            { label: t("thAvg"), num: true },
            { label: t("thMax"), num: true },
            { label: t("thOver15"), num: true },
            { label: t("thOver60"), num: true },
          ],
          offenders.map(
            (item, index) => `
            <tr>
              <td class="rank">${index + 1}</td>
              <td><strong>${escapeHtml(item.codComercial || "-")}</strong><br /><span class="muted">${escapeHtml(item.productName || item.codProduct || "-")}</span></td>
              <td class="route">${escapeHtml(stationPair(item.origin, item.destination, null, null))}</td>
              <td class="num">${numberFmt.format(item.observations || 0)}</td>
              <td class="num">${fmt1(item.avgDelay)}</td>
              <td class="num">${item.maxDelay || 0}</td>
              ${pctCell(item.delayed15Pct)}
              ${pctCell(item.severe60Pct)}
            </tr>
          `,
          ),
        );
};

const niceCeil = (value) => {
  if (value <= 0) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  const step = magnitude * (value / magnitude <= 2 ? 0.5 : value / magnitude <= 5 ? 1 : 2);
  return Math.ceil(value / step) * step;
};

const renderTrendChart = (trend) => {
  const points = (Array.isArray(trend) ? trend : []).filter((p) => p && p.day);
  if (points.length === 0) {
    trendChartEl.innerHTML = `<p class="chart-empty">${escapeHtml(t("trendNoData"))}</p>`;
    return;
  }

  const W = 640;
  const H = 240;
  const padL = 46;
  const padR = 14;
  const padT = 12;
  const padB = 30;
  const avg = points.map((p) => Math.max(0, Number(p.weighted_avg_delay ?? 0)));
  const peak = points.map((p) => Math.max(0, Number(p.peak_delay ?? 0)));
  const maxY = niceCeil(Math.max(1, ...peak, ...avg));
  const x = (i) => (points.length === 1 ? padL + (W - padL - padR) / 2 : padL + (i / (points.length - 1)) * (W - padL - padR));
  const y = (v) => padT + (1 - v / maxY) * (H - padT - padB);
  const line = (arr) => arr.map((v, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  const area = (arr) => `${line(arr)} L${x(arr.length - 1).toFixed(1)},${y(0).toFixed(1)} L${x(0).toFixed(1)},${y(0).toFixed(1)} Z`;

  const ticks = [0, 0.25, 0.5, 0.75, 1].map((k) => maxY * k);
  const gridHtml = ticks
    .map(
      (v) =>
        `<line class="grid" x1="${padL}" x2="${W - padR}" y1="${y(v).toFixed(1)}" y2="${y(v).toFixed(1)}" /><text class="axis-text" x="${padL - 6}" y="${(y(v) + 4).toFixed(1)}" text-anchor="end">${Math.round(v)} ${t("minutes")}</text>`,
    )
    .join("");

  const labelEvery = Math.max(1, Math.ceil(points.length / 8));
  const labelsHtml = points
    .map((p, i) =>
      i % labelEvery === 0 || i === points.length - 1
        ? `<text class="axis-text" x="${x(i).toFixed(1)}" y="${H - 8}" text-anchor="middle">${escapeHtml(String(p.day).slice(5).split("-").reverse().join("/"))}</text>`
        : "",
    )
    .join("");

  const dotsHtml =
    points.length <= 16
      ? avg.map((v, i) => `<circle class="dot" cx="${x(i).toFixed(1)}" cy="${y(v).toFixed(1)}" r="3.5"><title>${escapeHtml(points[i].day)}: ${fmt1(v)} ${t("minutes")} · ${t("trendPeak")} ${peak[i]}</title></circle>`).join("")
      : "";

  trendChartEl.innerHTML = `
    <svg class="trend-svg" viewBox="0 0 ${W} ${H}" role="img" aria-label="${escapeHtml(t("trendTitle"))}">
      ${gridHtml}
      <path class="area-peak" d="${area(peak)}" />
      <path class="line-peak" d="${line(peak)}" />
      <path class="area-avg" d="${area(avg)}" />
      <path class="line-avg" d="${line(avg)}" />
      ${dotsHtml}
      ${labelsHtml}
    </svg>
  `;
};

const findingHtml = (iconName, name, headline, meta, headlineClass = "") => `
  <div class="finding">
    ${icon(iconName)}
    <div>
      <div class="name">${escapeHtml(name)}</div>
      <div class="headline ${headlineClass}">${headline}</div>
      ${meta ? `<div class="meta">${meta}</div>` : ""}
    </div>
  </div>
`;

const renderHighlights = (historical) => {
  historicalHighlightsEl.classList.remove("skeleton-list");
  const topType = historical.topProblematicProduct;
  const topCorridor = historical.topProblematicCorridor;
  const summary = historical.accountability?.summary;
  const rows = [];

  if (topType) {
    rows.push(
      findingHtml(
        "train",
        t("histTopType"),
        escapeHtml(topType.productName || "-"),
        `${formatLargeInt(topType.accumulatedDelayMinutes)} ${t("minutes")} · ${numberFmt.format(topType.observations || 0)} ${t("histObsShort")} · ${numberFmt.format(topType.trains || 0)} ${t("histTrainsShort")}`,
      ),
    );
  }

  if (topCorridor) {
    rows.push(
      findingHtml(
        "route",
        t("histTopCorridor"),
        `${escapeHtml(topCorridor.corridor || t("histNoCorridor"))} ${sourceBadge(topCorridor.source)}`,
        `${formatLargeInt(topCorridor.accumulatedDelayMinutes)} ${t("minutes")} · ${numberFmt.format(topCorridor.observations || 0)} ${t("histObsShort")} · ${numberFmt.format(topCorridor.trains || 0)} ${t("histTrainsShort")}`,
        "accent",
      ),
    );
  }

  if (summary) {
    const trend = summary.worseningVs7d?.trend;
    const trendKey =
      trend === "worsening" ? "accountabilityTrendWorsening" : trend === "improving" ? "accountabilityTrendImproving" : "accountabilityTrendFlat";
    const delta = Number(summary.worseningVs7d?.deltaDelayed15Pct || 0);
    rows.push(
      findingHtml(
        "activity",
        t("accountabilityTrendLabel"),
        `<span class="trend-badge ${escapeHtml(trend || "flat")}">${escapeHtml(t(trendKey))} ${delta > 0 ? "+" : ""}${delta.toFixed(1)} pp</span>`,
        `${metricLabelWithInfo(t("accountabilityDelayed15"), t("tipDelayed15"), "accountability-delayed15")} ${fmt1(summary.delayed15Pct)}% · ${metricLabelWithInfo(t("accountabilitySevere60"), t("tipSevere60"), "accountability-severe60")} ${fmt1(summary.severe60Pct)}%`,
      ),
    );
  }

  historicalHighlightsEl.innerHTML = rows.length > 0 ? rows.join("") : emptyRow(t("histNoData"));
};

const renderHistorical = (historical) => {
  historicalProductsEl.classList.remove("skeleton-bars");
  renderHistoryRange(historical);

  if (!historical || !historical.summary) {
    historicalCardsEl.innerHTML = "";
    historicalHighlightsEl.classList.remove("skeleton-list");
    historicalHighlightsEl.innerHTML = emptyRow(t("histNoData"));
    historicalProductsEl.innerHTML = "";
    trendChartEl.innerHTML = "";
    renderAccountability(null);
    return;
  }

  const summary = historical.summary;
  const ingestion = historical.ingestion || {};
  const severe = Number(summary.severePct || 0);

  historicalCardsEl.innerHTML = [
    { icon: "database", label: t("histCardObservations"), value: numberFmt.format(summary.observations || 0) },
    { icon: "train", label: t("histCardUniqueTrains"), value: numberFmt.format(summary.uniqueTrains || 0) },
    { icon: "clock", label: t("histCardAvgDelay"), value: `${fmt1(summary.avgDelay)} ${t("minutes")}` },
    { icon: "alert", iconTone: "danger", label: t("histCardMaxDelay"), value: `${summary.maxDelay || 0} ${t("minutes")}` },
    {
      icon: "sigma",
      labelHtml: metricLabelWithInfo(t("histCardAccumDelay"), t("tipAccumDelay"), "hist-accum-delay"),
      value: `${formatLargeInt(summary.accumulatedDelayMinutes)} ${t("minutes")}`,
    },
    { icon: "check", iconTone: "success", label: t("histCardOnTimePct"), value: `${fmt1(summary.onTimePct)}%` },
    { icon: "alert-circle", iconTone: severe >= 5 ? "danger" : severe >= 1 ? "warning" : "", label: t("histCardSeverePct"), value: `${fmt1(severe)}%` },
    {
      icon: "activity",
      labelHtml: metricLabelWithInfo(t("histCardAvgBatch"), t("tipAvgTrainsPerBatch"), "hist-avg-batch"),
      value: numberFmt.format(Math.round(ingestion.avgTrainsPerBatch || 0)),
    },
  ]
    .map(cardHtml)
    .join("");

  renderHighlights(historical);
  renderTrendChart(historical.dailyTrend);

  const productFilter = normalizeText(state.historicalProductsQuery.trim());
  const byProduct = (Array.isArray(historical.byProduct) ? historical.byProduct : []).filter((item) =>
    matchesFilter(productFilter, item.productName || item.productNameEs || item.codProduct || item.cod_product),
  );

  if (byProduct.length === 0) {
    historicalProductsEl.innerHTML = emptyRow(t("noLocalRows"));
  } else {
    const maxObs = byProduct.reduce((max, item) => Math.max(max, Number(item.observations || 0)), 1);
    renderBarRows(
      historicalProductsEl,
      byProduct.slice(0, 8).map((item) => {
        const avg = Number(item.avg_delay ?? item.avgDelay ?? 0);
        return {
          label: `${item.productName || item.productNameEs || item.codProduct || "-"} (${item.codProduct ?? item.cod_product ?? "-"})`,
          value: numberFmt.format(item.observations || 0),
          extra: `${fmt1(avg)} ${t("minutes")}`,
          pct: ((item.observations || 0) / maxObs) * 100,
          tone: toneForAvg(avg),
        };
      }),
    );
  }

  renderAccountability(historical.accountability ?? null);
};

const renderTrains = (data) => {
  state.trainsItems = Array.isArray(data.items) ? data.items : [];
  const sortedItems = sortTrains(state.trainsItems);

  if (sortedItems.length === 0) {
    trainsBodyEl.innerHTML = `<tr><td colspan="7">${escapeHtml(t("noRows"))}</td></tr>`;
    return;
  }

  trainsBodyEl.innerHTML = sortedItems
    .map((train) => {
      const delay = Number(train.ult_retraso ?? 0);
      return `
        <tr>
          <td><button class="train-map-link" type="button" data-map-train="${escapeHtml(train.cod_comercial)}" aria-label="${escapeHtml(t("mapViewTrain").replace("{train}", train.cod_comercial))}" ${isValidMapPosition(train) ? "" : "disabled"}>${escapeHtml(train.cod_comercial)}</button></td>
          <td>${escapeHtml(train.product_name)}</td>
          <td>${escapeHtml(train.des_corridor || "-")}</td>
          <td>${escapeHtml(stationPair(train.origin_name, train.destination_name, train.cod_origen, train.cod_destino))}</td>
          <td>${escapeHtml(nextStation(train.next_station_name, train.cod_est_sig, train.hora_llegada_sig_est))}</td>
          <td><span class="delay ${delayClass(delay)}">${escapeHtml(delayLabel(delay))}</span></td>
          <td class="nowrap">${escapeHtml(asTime(train.last_seen_at))}</td>
        </tr>
      `;
    })
    .join("");
};

let trainMap = null;
let mapHasFitted = false;
let selectedMapTrainId = null;
const trainMarkers = new Map();
let mapVectorLayer = null;
let mapZoneLayer = null;
let mapAnalysisKey = null;
let mapZonePeriod = "history";
const visibleMapZones = () => {
  const analysis = latestDashboard?.mapAnalytics;
  return analysis?.version === 3 ? (mapZonePeriod === "history" ? analysis.history.zones : analysis.zones) : [];
};
const mapZoneName = (zone) => zone.nearbyStation ? t("mapZoneNear").replace("{station}", zone.nearbyStation) : zone.id;
const mapZoneTime = (epoch) => new Date(epoch * 1000).toLocaleString(locale(), { dateStyle: "medium", timeStyle: "short" });
const mapStyleUrl = () => `https://tiles.openfreemap.org/styles/${document.documentElement.dataset.theme === "dark" ? "dark" : "positron"}`;
const updateMapTheme = () => {
  if (mapVectorLayer) mapVectorLayer.getMaplibreMap().setStyle(mapStyleUrl());
};

const renderMapAnalytics = () => {
  const analysis = latestDashboard?.mapAnalytics?.version === 3 ? latestDashboard.mapAnalytics : null;
  const zones = visibleMapZones();
  mapFitBtnEl.disabled = trainMarkers.size === 0 && zones.length === 0;
  $("#map-zones-count").textContent = analysis ? numberFmt.format(zones.length) : "—";
  const range = mapZonePeriod === "history" ? analysis?.history : analysis;
  $("#map-analysis-status").textContent = analysis ? `${asDate(range.coveredSinceEpoch ?? range.sinceEpoch)} → ${asDate(range.untilEpoch)} · ${t("mapAnalysisReady").replace("{time}", asTime(analysis.untilEpoch))}${mapZonePeriod === "history" && !analysis.history.complete ? ` · ${t("mapZoneBackfill").replace("{done}", analysis.history.processedDays).replace("{total}", analysis.history.totalDays)}` : ""}` : t("mapAnalysisPending");
  $("#map-zone-period").setAttribute("aria-label", t("mapZonePeriod"));
  $("#map-zone-ranking-title").textContent = t(mapZonePeriod === "history" ? "mapZoneWorst" : "mapZoneLatest");
  for (const button of document.querySelectorAll("[data-map-period]")) {
    const active = button.dataset.mapPeriod === mapZonePeriod;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", String(active));
  }
  const key = `${analysis?.generatedAt ?? "pending"}|${state.lang}|${mapZonePeriod}`;
  if (mapAnalysisKey === key) return;
  mapAnalysisKey = key;
  const openZoneId = mapZoneLayer.getLayers().find((layer) => layer.isPopupOpen())?.options.zoneId;
  mapZoneLayer.clearLayers();
  $("#map-zone-ranking").innerHTML = zones.slice(0, 5).map((zone) => `<li><button type="button" class="map-zone-link" data-map-zone="${escapeHtml(zone.id)}"><strong>${escapeHtml(mapZoneName(zone))}</strong><span>${escapeHtml(numberFmt.format(zone.trainCount))} ${escapeHtml(t("mapZoneTrains").toLowerCase())} · +${escapeHtml(numberFmt.format(zone.gainedDelayMinutes))} min</span><small>${escapeHtml(t("mapZoneLast"))}: ${escapeHtml(mapZoneTime(zone.lastDelayAt))}</small></button></li>`).join("");
  $("#map-zone-empty").textContent = analysis ? t("mapZoneEmpty") : t("mapAnalysisPending");
  $("#map-zone-empty").classList.toggle("hidden", zones.length > 0);
  for (const zone of zones) {
    const L = window.L;
    if (!zone.railSegments?.length) continue;
    L.polyline(zone.railSegments, { color: "#fff", weight: 6, opacity: 0.8, smoothFactor: 0, interactive: false }).addTo(mapZoneLayer);
    const area = L.polyline(zone.railSegments, { color: "#09090b", weight: 3, opacity: 1, smoothFactor: 0, zoneId: zone.id });
    const fields = [
      [t("mapZoneTrains"), numberFmt.format(zone.trainCount)],
      [t("mapGainedDelay"), numberFmt.format(zone.gainedDelayMinutes)],
      [t("mapZoneAverageAdded"), `${numberFmt.format(zone.averageAddedDelayMinutes)} min`],
      [t("mapZonePeak"), `${numberFmt.format(zone.maxDelayMinutes)} min`],
      [t("mapZoneLast"), mapZoneTime(zone.lastDelayAt)],
    ];
    area.bindPopup(`<strong>${escapeHtml(t("mapZoneTitle"))}</strong><p class="map-zone-explanation">${escapeHtml(mapZoneName(zone))}</p><dl class="map-zone-fields">${fields.map(([label, value]) => `<div><dt>${escapeHtml(label)}</dt><dd>${escapeHtml(value)}</dd></div>`).join("")}</dl>`, { className: "map-zone-popup", maxWidth: 300 });
    area.on("click", () => selectMapTrain(""));
    area.on("add", () => {
      const element = area.getElement();
      element.classList.add("map-black-zone");
      element.setAttribute("tabindex", "0");
      element.setAttribute("role", "button");
      element.setAttribute("aria-label", `${t("mapZoneTitle")} · ${zone.trainCount} ${t("mapZoneTrains")}`);
      element.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          selectMapTrain("");
          area.openPopup();
        }
      });
    }).addTo(mapZoneLayer);
    if (openZoneId === zone.id) area.openPopup();
  }
};

const isValidMapPosition = (train) =>
  Number.isFinite(train.latitud) && Number.isFinite(train.longitud) &&
  Math.abs(train.latitud) <= 85.05112878 && Math.abs(train.longitud) <= 180 &&
  (train.latitud !== 0 || train.longitud !== 0);

const updateMapZoom = () => {
  const zoom = trainMap.getZoom();
  mapZoomInEl.disabled = zoom >= trainMap.getMaxZoom();
  mapZoomOutEl.disabled = zoom <= trainMap.getMinZoom();
  $("#train-map").classList.toggle("map-overview", zoom < 9);
};

const renderMapTrainDetail = () => {
  const train = latestDashboard?.mapTrains?.find((item) => item.cod_comercial === selectedMapTrainId);
  const selected = Boolean(train && trainMarkers.has(selectedMapTrainId));
  if (!selected) selectedMapTrainId = null;
  mapTrainSelectEl.value = selectedMapTrainId ?? "";
  if (!selected && mapTrainDetailEl.contains(document.activeElement)) {
    (mapTrainSelectEl.disabled ? $("#train-map") : mapTrainSelectEl).focus({ preventScroll: true });
  }
  mapTrainDetailEl.classList.toggle("hidden", !selected);
  for (const [id, marker] of trainMarkers) {
    marker.getElement().classList.toggle("selected", id === selectedMapTrainId);
    marker.getElement().setAttribute("aria-pressed", String(id === selectedMapTrainId));
    marker.setZIndexOffset(id === selectedMapTrainId ? 1000 : 0);
  }
  if (!selected) {
    mapDetailContentEl.replaceChildren();
    mapLocationLinkEl.removeAttribute("href");
    return;
  }
  $("#map-detail-heading").textContent = `${train.product_name} · ${train.cod_comercial}`;
  const motion = latestDashboard?.mapAnalytics?.trains.find((item) =>
    item.codComercial === train.cod_comercial && item.codProduct === train.cod_product &&
    item.codOrigen === train.cod_origen && item.codDestino === train.cod_destino &&
    item.lastGpsAt >= train.first_seen_at - 180);
  const freshSpeed = motion && Date.now() / 1000 - motion.lastGpsAt <= 180 ? motion.latestSpeedKmh : null;
  const speedLabel = (value) => value !== null && value !== undefined ? `${numberFmt.format(value)} km/h` : t("mapNoSpeed");
  const fields = [
    [t("mapTrainAverageSpeed"), speedLabel(motion?.averageSpeedKmh)],
    [t("mapLatestSpeed"), speedLabel(freshSpeed)],
    [t("mapObserved"), motion ? numberFmt.format(motion.observedMinutes) : "—"],
    [t("mapGainedDelay"), motion ? numberFmt.format(motion.gainedDelayMinutes) : "—"],
    [t("mapAverageDelay"), motion?.averageDelayMinutes !== null && motion?.averageDelayMinutes !== undefined ? `${numberFmt.format(motion.averageDelayMinutes)} ${t("minutes")}` : "—"],
    [t("thRoute"), stationPair(train.origin_name, train.destination_name, train.cod_origen, train.cod_destino)],
    [t("thNext"), nextStation(train.next_station_name, train.cod_est_sig, train.hora_llegada_sig_est)],
    [t("thCorridor"), train.des_corridor || "-"],
    [t("mapCoordinates"), `${train.latitud.toFixed(6)}, ${train.longitud.toFixed(6)}`],
    [t("mapGps"), asTime(train.gps_time)],
    [t("mapReceived"), asTime(train.last_seen_at)],
  ];
  const scrollTop = mapDetailContentEl.scrollTop;
  mapDetailContentEl.innerHTML = `<span class="delay ${delayClass(train.ult_retraso)}">${escapeHtml(delayLabel(train.ult_retraso))}</span>
    <dl>${fields.map(([label, value]) => `<div><dt>${escapeHtml(label)}</dt><dd>${escapeHtml(value)}</dd></div>`).join("")}</dl>`;
  mapDetailContentEl.scrollTop = scrollTop;
  mapLocationLinkEl.href = `https://www.openstreetmap.org/?mlat=${train.latitud}&mlon=${train.longitud}#map=16/${train.latitud}/${train.longitud}`;
};

const panToSelectedMapTrain = (closeUp = false) => {
  const marker = trainMarkers.get(selectedMapTrainId);
  if (!marker) return;
  const position = marker.getLatLng();
  if (closeUp) trainMap.setView(position, 16, { animate: false });
  const mobile = window.matchMedia("(max-width: 672px)").matches;
  trainMap.panInside(position, {
    paddingTopLeft: [24, 24],
    paddingBottomRight: mobile ? [24, mapTrainDetailEl.offsetHeight + 60] : [mapTrainDetailEl.offsetWidth + 36, 24],
    animate: false,
  });
};

const selectMapTrain = (id, closeUp = false) => {
  if (id && !trainMarkers.has(id)) return;
  if (selectedMapTrainId !== (id || null)) mapDetailContentEl.scrollTop = 0;
  selectedMapTrainId = id || null;
  if (selectedMapTrainId) trainMap.closePopup();
  renderMapTrainDetail();
  panToSelectedMapTrain(closeUp);
};

const fitTrainMap = () => {
  if (!trainMap) return;
  const points = [...trainMarkers.values()].map((marker) => marker.getLatLng());
  if ($("#map-zones-visible").checked) for (const zone of visibleMapZones()) points.push(...zone.bounds);
  if (points.length === 0) return;
  trainMap.fitBounds(points, {
    padding: [24, 24], maxZoom: 8, animate: false,
  });
};

const isMapDataStale = (ingestor) =>
  ingestor.lastError || ingestor.lastSource === "cache-local" || !ingestor.lastSuccessAt ||
  Date.now() / 1000 - ingestor.lastSuccessAt > ingestor.pollingIntervalMs / 1000 * 2;

const renderTrainMap = (items, ingestor) => {
  const L = window.L;
  if (!trainMap) {
    trainMap = L.map("train-map", { scrollWheelZoom: true, zoomControl: false, minZoom: 3, maxZoom: 19 }).setView([40, -3.7], 6);
    trainMap.on("zoomend", updateMapZoom);
    trainMap.on("popupopen", (event) => {
      const close = event.popup.getElement().querySelector(".leaflet-popup-close-button");
      close.setAttribute("aria-label", t("mapClose"));
      close.title = t("mapClose");
    });
    updateMapZoom();
    let webgl = null;
    try { webgl = document.createElement("canvas").getContext("webgl2"); } catch {}
    if (webgl) {
      webgl.getExtension("WEBGL_lose_context")?.loseContext();
      mapVectorLayer = L.maplibreGL({ style: mapStyleUrl(), attributionControl: { customAttribution: '<a href="https://openfreemap.org/">OpenFreeMap</a> · <a href="https://www.openmaptiles.org/">OpenMapTiles</a> · &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>' } }).addTo(trainMap);
      mapVectorLayer.getMaplibreMap().on("error", () => mapTileErrorEl.classList.remove("hidden"));
      mapVectorLayer.getMaplibreMap().on("idle", () => mapTileErrorEl.classList.add("hidden"));
    } else {
      L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
        maxZoom: 19, attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
      }).on("loading", () => mapTileErrorEl.classList.add("hidden"))
        .on("tileerror", () => mapTileErrorEl.classList.remove("hidden")).addTo(trainMap);
    }
    mapZoneLayer = L.layerGroup().addTo(trainMap);
  }
  renderMapAnalytics();

  const valid = items.filter(isValidMapPosition);
  const activeIds = new Set(valid.map((train) => train.cod_comercial));
  for (const [id, marker] of trainMarkers) {
    if (!activeIds.has(id)) {
      marker.remove();
      trainMarkers.delete(id);
    }
  }

  // shortcut: co-located markers overlap; the train picker selects each one without clustering.
  for (const train of valid) {
    const delay = train.ult_retraso;
    const label = `${t("thTrain")} ${train.cod_comercial} · ${train.product_name} · ${delayLabel(delay)}`;
    const markerClass = `train-map-marker ${delayClass(delay)}`;
    const markerIcon = L.divIcon({ className: markerClass, html: icon("train"), iconSize: [32, 32], iconAnchor: [16, 16] });
    let marker = trainMarkers.get(train.cod_comercial);
    if (marker) {
      marker.setLatLng([train.latitud, train.longitud]);
      if (marker.options.icon.options.className !== markerClass) marker.setIcon(markerIcon);
    } else {
      marker = L.marker([train.latitud, train.longitud], { icon: markerIcon, title: label, riseOnHover: true })
        .on("click", () => selectMapTrain(train.cod_comercial)).addTo(trainMap);
      trainMarkers.set(train.cod_comercial, marker);
    }
    marker.getElement().title = label;
    marker.getElement().setAttribute("aria-label", label);
  }

  mapTrainSelectEl.innerHTML = `<option value="">${escapeHtml(t("mapSelectPlaceholder"))}</option>` +
    [...valid].sort((a, b) => String(a.cod_comercial).localeCompare(String(b.cod_comercial), locale(), { numeric: true }))
      .map((train) => `<option value="${escapeHtml(train.cod_comercial)}">${escapeHtml(train.cod_comercial)} · ${escapeHtml(train.product_name)} · ${escapeHtml(delayLabel(train.ult_retraso))}</option>`).join("");
  mapTrainSelectEl.disabled = valid.length === 0;
  renderMapTrainDetail();
  mapFitBtnEl.disabled = trainMarkers.size === 0 && visibleMapZones().length === 0;
  if (!mapHasFitted && latestDashboard?.mapAnalytics && (trainMarkers.size > 0 || visibleMapZones().length > 0)) {
    fitTrainMap();
    mapHasFitted = true;
  }
  mapStatusEl.textContent = isMapDataStale(ingestor) ? t("mapStale") : valid.length === 0 ? t("mapEmpty") :
    `${t("mapCount").replace("{shown}", numberFmt.format(valid.length)).replace("{total}", numberFmt.format(items.length))} · ${t("statusLastUpdate")}: ${asTime(ingestor.lastSuccessAt)}`;
};

mapFitBtnEl.addEventListener("click", fitTrainMap);
mapZoomInEl.addEventListener("click", () => trainMap?.zoomIn());
mapZoomOutEl.addEventListener("click", () => trainMap?.zoomOut());
mapTrainSelectEl.addEventListener("change", () => selectMapTrain(mapTrainSelectEl.value, true));
$("#map-zone-period").addEventListener("click", (event) => {
  const button = event.target.closest("[data-map-period]");
  if (!button) return;
  mapZonePeriod = button.dataset.mapPeriod;
  if (trainMap) renderMapAnalytics();
});
$("#map-zone-ranking").addEventListener("click", (event) => {
  const button = event.target.closest("[data-map-zone]");
  if (!button || !trainMap) return;
  const zone = visibleMapZones().find((item) => item.id === button.dataset.mapZone);
  if (!zone) return;
  selectMapTrain("");
  $("#map-zones-visible").checked = true;
  mapZoneLayer.addTo(trainMap);
  trainMap.fitBounds(zone.bounds, { padding: [32, 32], maxZoom: 13, animate: false });
  mapZoneLayer.getLayers().find((layer) => layer.options.zoneId === zone.id)?.openPopup();
  $("#train-map").scrollIntoView({ block: "center" });
});
$("#map-zones-visible").addEventListener("change", (event) => {
  if (!trainMap) return;
  if (event.target.checked) mapZoneLayer.addTo(trainMap);
  else mapZoneLayer.remove();
});
$("#train-map").addEventListener("wheel", (event) => {
  if (!event.ctrlKey) event.stopImmediatePropagation();
}, { capture: true });
$("#map-detail-close").addEventListener("click", () => selectMapTrain(""));
$(".map-layout").addEventListener("keydown", (event) => {
  if (event.key === "Escape" && selectedMapTrainId) {
    event.preventDefault();
    selectMapTrain("");
  }
});
$("#train-map").addEventListener("keydown", (event) => {
  const marker = event.target.closest(".train-map-marker");
  if (marker && (event.key === "Enter" || event.key === " ")) {
    event.preventDefault();
    marker.click();
  }
});
$("#map-locate-train").addEventListener("click", () => panToSelectedMapTrain(true));
trainsBodyEl.addEventListener("click", (event) => {
  const button = event.target.closest("[data-map-train]");
  if (!button) return;
  selectMapTrain(button.dataset.mapTrain, true);
  $("#mapa").scrollIntoView({ block: "start", behavior: "smooth" });
  $("#train-map").focus({ preventScroll: true });
});

const rerenderLocalFilteredPanels = () => {
  if (!latestDashboard) {
    return;
  }
  renderCorridors(Array.isArray(latestDashboard.topCorridors) ? latestDashboard.topCorridors : []);
  renderHistorical(latestDashboard.historical ?? null);
};

const loadDashboard = async () => {
  const params = new URLSearchParams();
  params.set("historyHours", String(state.historyHours));
  params.set("limit", String(state.limit));
  params.set("offset", String(state.offset));
  if (state.query.trim()) params.set("q", state.query.trim());
  if (state.minDelay.trim()) params.set("minDelay", state.minDelay.trim());
  if (state.historyFrom) params.set("historyFrom", state.historyFrom);
  if (state.historyTo) params.set("historyTo", state.historyTo);

  const data = await readJsonResponse(await apiFetch(`/api/dashboard?${params.toString()}`));

  latestDashboard = data;
  state.lastAppliedSuccessAt = Number(data?.ingestor?.lastSuccessAt || state.lastAppliedSuccessAt || 0);
  setPendingSync(false);

  renderOverviewCards(data.overview, data.today, data.typeInsights);
  overviewCardsEl.setAttribute("aria-busy", "false");
  renderDelayBuckets(data.delayBuckets);
  renderProducts(data.byProduct);
  renderCorridors(data.topCorridors);
  renderToday(data.today);
  if (data.historical) renderHistorical(data.historical);
  if (data.trains) renderTrains(data.trains);
  renderTrainMap(data.mapTrains ?? [], data.ingestor);

  lastSeenEl.textContent = `${t("lastSignal")}: ${
    data.overview.lastSeenAtIso ? new Date(data.overview.lastSeenAtIso).toLocaleString(locale(), { hour12: false }) : "-"
  }`;

  const ingest = data.ingestor;
  if (ingest.lastError) {
    setStatus(`${t("statusError")}: ${ingest.lastError}`, "error");
    return;
  }

  const updatedAt =
    (ingest.lastSuccessAt ? new Date(ingest.lastSuccessAt * 1000) : null) ??
    (data.overview.lastSeenAtIso ? new Date(data.overview.lastSeenAtIso) : null);
  setStatus(
    updatedAt ? `${t("statusLastUpdate")}: ${updatedAt.toLocaleString(locale(), { hour12: false })}` : t("statusNoUpdate"),
    "ok",
  );
};

const trainsParams = (limit, offset) => {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  params.set("offset", String(offset));
  if (state.query.trim()) params.set("q", state.query.trim());
  if (state.minDelay.trim()) params.set("minDelay", state.minDelay.trim());
  return params;
};

const checkForPendingSync = async () => {
  try {
    const response = await fetch(buildUrl("/api/health"), {
      method: "GET",
      cache: "no-store",
      headers: { "accept-language": state.lang },
    });
    const data = await readJsonResponse(response);
    if (Number(data?.ingestor?.lastSuccessAt || 0) > state.lastAppliedSuccessAt) {
      setPendingSync(true);
      await refresh();
    }
  } catch {
    // noop: non-blocking polling
  }
};

let refreshPromise = null;
let refreshAgain = false;
const refresh = () => {
  if (refreshPromise) {
    refreshAgain = true;
    return refreshPromise;
  }
  refreshPromise = (async () => {
    do {
      refreshAgain = false;
      await loadDashboard();
    } while (refreshAgain);
  })()
    .catch((error) => {
      setStatus(error instanceof Error ? error.message : String(error), "error");
      mapStatusEl.textContent = t("mapStale");
    })
    .finally(() => {
      refreshPromise = null;
    });
  return refreshPromise;
};

let eventsConnected = false;
const events = new EventSource(buildUrl("/api/events"));
events.onopen = () => {
  eventsConnected = true;
  if (latestDashboard && !document.hidden) void refresh();
};
events.onerror = () => {
  eventsConnected = false;
};
events.onmessage = (event) => {
  const update = JSON.parse(event.data);
  if (
    !document.hidden &&
    latestDashboard &&
    (Number(update.lastSuccessAt) > state.lastAppliedSuccessAt || update.kind === "history")
  ) {
    void refresh();
  }
};
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) void checkForPendingSync();
});

const toCsvCell = (value) => `"${String(value ?? "").replaceAll('"', '""')}"`;

const toExportRows = (items) =>
  sortTrains(items).map((train) => ({
    train: train.cod_comercial ?? "",
    type: train.product_name ?? "",
    corridor: train.des_corridor ?? "",
    route: stationPair(train.origin_name, train.destination_name, train.cod_origen, train.cod_destino),
    next: nextStation(train.next_station_name, train.cod_est_sig, train.hora_llegada_sig_est),
    delay: Number(train.ult_retraso ?? 0),
    last: asTime(train.last_seen_at),
  }));

const fetchAllFilteredTrains = async () => {
  const items = [];
  const limit = 200;
  let offset = 0;
  let total = Infinity;

  while (offset < total) {
    const data = await readJsonResponse(await apiFetch(`/api/trains?${trainsParams(limit, offset)}`));
    const pageItems = Array.isArray(data.items) ? data.items : [];
    total = Number(data.total ?? pageItems.length);
    items.push(...pageItems);
    if (pageItems.length === 0) {
      break;
    }
    offset += pageItems.length;
  }

  return items;
};

const downloadFile = (filename, mimeType, content) => {
  const url = URL.createObjectURL(new Blob([content], { type: mimeType }));
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
};

const exportHeaders = () => [t("thTrain"), t("thType"), t("thCorridor"), t("thRoute"), t("thNext"), t("thDelay"), t("thLast")];
const exportFilename = (ext) => `trenes-activos-${new Date().toISOString().slice(0, 19).replaceAll(":", "-")}.${ext}`;

const runExport = async (build) => {
  try {
    const rows = toExportRows(await fetchAllFilteredTrains());
    build(rows);
    setStatus(t("exportDone"), "ok");
  } catch (error) {
    setStatus(`${t("exportFail")}: ${error instanceof Error ? error.message : String(error)}`, "error");
  }
};

filtersForm.addEventListener("submit", (event) => {
  event.preventDefault();
  state.query = searchInput.value;
  state.minDelay = minDelayInput.value;
  state.offset = 0;
  void refresh();
});

langSwitchEl.addEventListener("change", () => {
  state.lang = langSwitchEl.value === "en" ? "en" : "es";
  localStorage.setItem("retrasometro_lang", state.lang);
  applyStaticTexts();
  void refresh();
});

for (const button of historyButtons) {
  button.addEventListener("click", () => {
    const hours = Number(button.dataset.hours);
    if (!Number.isFinite(hours) || hours < 1) {
      return;
    }
    state.historyHours = hours;
    state.historyFrom = "";
    state.historyTo = "";
    localStorage.setItem("retrasometro_history_hours", String(hours));
    setHistoryButtonsActive();
    historyFromInputEl.value = "";
    historyToInputEl.value = "";
    void refresh();
  });
}

historyRangeToggleEl.addEventListener("click", () => {
  const open = historyRangeEl.classList.toggle("hidden");
  historyRangeToggleEl.setAttribute("aria-expanded", String(!open));
});

historyRangeApplyBtnEl.addEventListener("click", () => {
  state.historyFrom = historyFromInputEl.value || "";
  state.historyTo = historyToInputEl.value || "";
  setHistoryButtonsActive();
  setStatus(t("histRangeApplied"), "ok");
  void refresh();
});

historyRangeClearBtnEl.addEventListener("click", () => {
  state.historyFrom = "";
  state.historyTo = "";
  historyFromInputEl.value = "";
  historyToInputEl.value = "";
  setHistoryButtonsActive();
  setStatus(t("histRangeCleared"), "ok");
  void refresh();
});

apiDocsBtnEl.addEventListener("click", () => {
  if (!window.open(buildUrl("/api-docs.html"), "_blank", "noopener,noreferrer")) {
    setStatus(t("docsOpenFail"), "error");
  }
});

syncBtnEl.addEventListener("click", () => {
  void refresh();
});

for (const button of sortButtons) {
  button.addEventListener("click", () => {
    const field = button.dataset.sort;
    if (state.sortBy === field) {
      state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
    } else {
      state.sortBy = field;
      state.sortDir = "asc";
    }
    renderSortIndicators();
    renderTrains({ items: state.trainsItems });
  });
}

const localFilters = [
  [corridorsSearchInputEl, "corridorsQuery"],
  [historicalProductsSearchInputEl, "historicalProductsQuery"],
  [accountabilityRoutesSearchInputEl, "accountabilityRoutesQuery"],
  [accountabilityTrainsSearchInputEl, "accountabilityTrainsQuery"],
];
for (const [input, key] of localFilters) {
  input.addEventListener("input", () => {
    state[key] = input.value || "";
    rerenderLocalFilteredPanels();
  });
}

exportCsvBtnEl.addEventListener("click", () =>
  runExport((rows) => {
    const body = rows.map((row) =>
      [row.train, row.type, row.corridor, row.route, row.next, row.delay, row.last].map(toCsvCell).join(";"),
    );
    downloadFile(
      exportFilename("csv"),
      "text/csv;charset=utf-8",
      `﻿${exportHeaders().map(toCsvCell).join(";")}\n${body.join("\n")}\n`,
    );
  }),
);

exportExcelBtnEl.addEventListener("click", () =>
  runExport((rows) => {
    const cells = (values, tag) => values.map((v) => `<${tag}>${escapeHtml(v)}</${tag}>`).join("");
    const htmlRows = rows
      .map((row) => `<tr>${cells([row.train, row.type, row.corridor, row.route, row.next, row.delay, row.last], "td")}</tr>`)
      .join("");
    downloadFile(
      exportFilename("xls"),
      "application/vnd.ms-excel;charset=utf-8",
      `﻿<!doctype html><html><head><meta charset="utf-8" /></head><body><table border="1"><thead><tr>${cells(exportHeaders(), "th")}</tr></thead><tbody>${htmlRows}</tbody></table></body></html>`,
    );
  }),
);

document.addEventListener("click", (event) => {
  if (!(event.target instanceof Element)) {
    return;
  }

  const trigger = event.target.closest(".info-trigger");
  if (trigger) {
    const wrapper = trigger.closest(".metric-info");
    const isOpen = wrapper.classList.contains("open");
    closeOpenTooltips();
    if (!isOpen) {
      wrapper.classList.add("open");
      trigger.setAttribute("aria-expanded", "true");
    }
    return;
  }

  if (!event.target.closest(".metric-info")) {
    closeOpenTooltips();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeOpenTooltips();
    if (document.activeElement instanceof HTMLElement && document.activeElement.classList.contains("info-trigger")) {
      document.activeElement.blur();
    }
  }
});

const boot = async () => {
  const themeController = initThemeController({ selectEl: themeSwitchEl, onThemeChange: updateMapTheme });
  themeSwitchEl.value = themeController.getMode();
  langSwitchEl.value = state.lang;
  applyStaticTexts();
  setStatus(t("statusLoading"));

  await refresh();

  setInterval(() => {
    if (!document.hidden && !eventsConnected) void checkForPendingSync();
    if (!document.hidden && latestDashboard && isMapDataStale(latestDashboard.ingestor)) {
      mapStatusEl.textContent = t("mapStale");
    }
  }, AUTO_REFRESH_MS);
};

void boot();
