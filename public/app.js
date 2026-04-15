import { initThemeController } from "./theme.js";

const overviewCardsEl = document.querySelector("#overview-cards");
const delayBucketsEl = document.querySelector("#delay-buckets");
const productsEl = document.querySelector("#products");
const corridorsEl = document.querySelector("#corridors");
const todayMetricsEl = document.querySelector("#today-metrics");
const trainsBodyEl = document.querySelector("#trains-body");
const corridorsSearchInputEl = document.querySelector("#corridors-search");
const historicalProductsSearchInputEl = document.querySelector("#historical-products-search");
const accountabilityRoutesSearchInputEl = document.querySelector("#accountability-routes-search");
const accountabilityTrainsSearchInputEl = document.querySelector("#accountability-trains-search");
const statusPillEl = document.querySelector("#status-pill");
const lastSeenEl = document.querySelector("#last-seen");
const historicalCardsEl = document.querySelector("#historical-cards");
const historicalHighlightsEl = document.querySelector("#historical-highlights");
const historicalProductsEl = document.querySelector("#historical-products");
const accountabilityRoutesEl = document.querySelector("#accountability-routes");
const accountabilityTrainsEl = document.querySelector("#accountability-trains");
const historyButtons = [...document.querySelectorAll(".history-btn")];
const historyFromInputEl = document.querySelector("#history-from");
const historyToInputEl = document.querySelector("#history-to");
const historyRangeApplyBtnEl = document.querySelector("#history-range-apply");
const historyRangeClearBtnEl = document.querySelector("#history-range-clear");

const filtersForm = document.querySelector("#filters");
const searchInput = document.querySelector("#search");
const minDelayInput = document.querySelector("#min-delay");
const syncBtnEl = document.querySelector("#sync-btn");
const exportCsvBtnEl = document.querySelector("#export-csv-btn");
const exportExcelBtnEl = document.querySelector("#export-excel-btn");
const sortButtons = [...document.querySelectorAll("[data-sort]")];

const langSwitchEl = document.querySelector("#lang-switch");
const themeSwitchEl = document.querySelector("#theme-switch");
const themeOptionSystemEl = document.querySelector("#theme-option-system");
const themeOptionLightEl = document.querySelector("#theme-option-light");
const themeOptionDarkEl = document.querySelector("#theme-option-dark");
const apiDocsBtnEl = document.querySelector("#api-docs-btn");
const rawToggleBtnEl = document.querySelector("#raw-toggle-btn");
const rawPanelEl = document.querySelector("#raw-panel");
const rawTitleEl = document.querySelector("#raw-title");
const rawMetaEl = document.querySelector("#raw-meta");
const rawJsonEl = document.querySelector("#raw-json");
const copyRawBtnEl = document.querySelector("#copy-raw-btn");
const closeRawBtnEl = document.querySelector("#close-raw-btn");
const recoverHistoryBtnEl = document.querySelector("#recover-history-btn");

const I18N = {
  es: {
    kicker: "retrasometro",
    title: "Retrasos ferroviarios en tiempo real",
    subtitle: "",
    langLabel: "Idioma",
    themeLabel: "Tema",
    themeSystem: "Sistema",
    themeLight: "Claro",
    themeDark: "Oscuro",
    apiDocs: "API",
    rawToggle: "Datos en bruto",
    rawToggleHide: "Ocultar datos en bruto",
    rawTitle: "Datos en bruto en tiempo real",
    rawCopy: "Copiar endpoint",
    rawClose: "Cerrar",
    recoverHistory: "Recuperar histórico",
    delayTitle: "Distribución de retrasos",
    productsTitle: "Tipos de tren activos",
    corridorsTitle: "Corredores con más tráfico",
    todayTitle: "Acumulado del día",
    historicalTitle: "Estadísticas históricas",
    historicalHighlightsTitle: "Hallazgos del período",
    historicalProductsTitle: "Distribución histórica por tipo",
    accountabilityRoutesTitle: "Rutas reincidentes",
    accountabilityTrainsTitle: "Trenes reincidentes",
    historyFromLabel: "Desde",
    historyToLabel: "Hasta",
    historyApplyRange: "Aplicar rango",
    historyClearRange: "Limpiar",
    trainsTitle: "Trenes activos",
    apply: "Aplicar",
    searchPlaceholder: "Buscar por tren, corredor o estación",
    minDelayPlaceholder: "Min retraso",
    thTrain: "Tren",
    thType: "Tipo",
    thCorridor: "Corredor",
    thRoute: "Origen -> Destino",
    thNext: "Prox. estación",
    thDelay: "Retraso",
    thLast: "Última señal",
    cardActive: "Trenes activos",
    cardAvg: "Retraso medio",
    cardMax: "Retraso máximo",
    cardOver15: "> 15 min",
    cardOver60: "> 60 min",
    cardAccessible: "Accesibles",
    cardObs: "Observaciones hoy",
    cardKm: "Km trazados hoy",
    cardProblematicType: "Tren más problemático (min acumulados)",
    cardVolumeType: "Tipo con más trenes/km",
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
    historicalProductsSearchPlaceholder: "Filtrar tipos históricos",
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
    routeSeparator: " -> ",
    keyRequestFailed: "No se pudo obtener clave API",
    rawGenerated: "Generado",
    rawCount: "Trenes",
    rawCoverage: "Cobertura 48h",
    rawMissingRuns: "huecos estimados",
    rawCopyOk: "Endpoint copiado",
    rawCopyFail: "No se pudo copiar el endpoint",
    recoverDone: "Recuperación completada",
    recoverFail: "No se pudo recuperar histórico",
    syncNow: "Sincronizar",
    syncAvailable: "Hay datos nuevos. Pulsa sincronizar.",
    exportCsv: "Exportar CSV",
    exportExcel: "Exportar Excel",
    exportDone: "Exportación completada",
    exportFail: "No se pudo exportar",
    sortAsc: "Ascendente",
    sortDesc: "Descendente",
    minutes: "min",
    docsOpenFail: "No se pudo abrir la documentación API",
  },
  en: {
    kicker: "retrasometro",
    title: "Real-time rail delays",
    subtitle: "",
    langLabel: "Language",
    themeLabel: "Theme",
    themeSystem: "System",
    themeLight: "Light",
    themeDark: "Dark",
    apiDocs: "API",
    rawToggle: "Raw data",
    rawToggleHide: "Hide raw data",
    rawTitle: "Raw live data",
    rawCopy: "Copy endpoint",
    rawClose: "Close",
    recoverHistory: "Recover history",
    delayTitle: "Delay distribution",
    productsTitle: "Active train types",
    corridorsTitle: "Top traffic corridors",
    todayTitle: "Today aggregate",
    historicalTitle: "Historical statistics",
    historicalHighlightsTitle: "Period highlights",
    historicalProductsTitle: "Historical distribution by type",
    accountabilityRoutesTitle: "Repeat routes",
    accountabilityTrainsTitle: "Repeat trains",
    historyFromLabel: "From",
    historyToLabel: "To",
    historyApplyRange: "Apply range",
    historyClearRange: "Clear",
    trainsTitle: "Active trains",
    apply: "Apply",
    searchPlaceholder: "Search by train, corridor or station",
    minDelayPlaceholder: "Min delay",
    thTrain: "Train",
    thType: "Type",
    thCorridor: "Corridor",
    thRoute: "Origin -> Destination",
    thNext: "Next station",
    thDelay: "Delay",
    thLast: "Last signal",
    cardActive: "Active trains",
    cardAvg: "Average delay",
    cardMax: "Max delay",
    cardOver15: "> 15 min",
    cardOver60: "> 60 min",
    cardAccessible: "Accessible",
    cardObs: "Observations today",
    cardKm: "Tracked km today",
    cardProblematicType: "Most problematic train (accumulated min)",
    cardVolumeType: "Top type by trains/km",
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
    historicalProductsSearchPlaceholder: "Filter historical types",
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
    routeSeparator: " -> ",
    keyRequestFailed: "Could not request API key",
    rawGenerated: "Generated",
    rawCount: "Trains",
    rawCoverage: "48h coverage",
    rawMissingRuns: "estimated missing slots",
    rawCopyOk: "Endpoint copied",
    rawCopyFail: "Could not copy endpoint",
    recoverDone: "Recovery completed",
    recoverFail: "Could not recover history",
    syncNow: "Sync",
    syncAvailable: "New data available. Click sync.",
    exportCsv: "Export CSV",
    exportExcel: "Export Excel",
    exportDone: "Export completed",
    exportFail: "Could not export",
    sortAsc: "Ascending",
    sortDesc: "Descending",
    minutes: "min",
    docsOpenFail: "Could not open API docs",
  },
};

const storageGet = (key, legacyKey = "") => {
  const value = localStorage.getItem(key);
  if (value !== null) {
    return value;
  }
  if (legacyKey) {
    return localStorage.getItem(legacyKey);
  }
  return null;
};

const state = {
  query: "",
  minDelay: "",
  limit: 120,
  offset: 0,
  lang: storageGet("retrasometro_lang", "renfe_lang") === "en" ? "en" : "es",
  historyHours: Number(storageGet("retrasometro_history_hours", "renfe_history_hours")) || 168,
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
  lastUserActivityAtMs: Date.now(),
  lastAppliedSuccessAt: 0,
  hasPendingSync: false,
  trainsItems: [],
  sortBy: "delay",
  sortDir: "desc",
  isRawOpen: false,
  rawTimer: null,
};

const AUTO_REFRESH_MS = 20_000;
const INACTIVITY_SYNC_MS = 60_000;

let latestDashboard = null;
let numberFmt = new Intl.NumberFormat("es-ES");
let themeController = null;

const t = (key) => I18N[state.lang][key] || key;
const locale = () => (state.lang === "es" ? "es-ES" : "en-US");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const asTime = (epochSeconds) => {
  if (!epochSeconds) {
    return "-";
  }

  const date = new Date(epochSeconds * 1000);
  return date.toLocaleString(locale(), {
    hour12: false,
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
};

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
  const safeLabel = escapeHtml(label);
  const safeInfo = escapeHtml(infoText);
  const tipId = `tip-${idSuffix}`;
  return `<span class="label-with-tip">${safeLabel}<span class="metric-info"><button type="button" class="info-trigger" aria-label="${escapeHtml(t("tooltipInfoAria"))}" aria-controls="${tipId}" aria-expanded="false">ⓘ</button><span id="${tipId}" class="info-popover" role="tooltip">${safeInfo}</span></span></span>`;
};

const closeOpenTooltips = () => {
  const wrappers = document.querySelectorAll(".metric-info.open");
  for (const wrapper of wrappers) {
    wrapper.classList.remove("open");
    const trigger = wrapper.querySelector(".info-trigger");
    if (trigger) {
      trigger.setAttribute("aria-expanded", "false");
    }
  }
};

const toBigIntLoose = (value) => {
  if (typeof value === "bigint") {
    return value;
  }

  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      return 0n;
    }
    return BigInt(Math.trunc(value));
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return 0n;
    }

    const intLike = trimmed.match(/^(-?\d+)(?:[.,]\d+)?$/);
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

const delayClass = (delay) => {
  if (delay < 0) return "ahead";
  if (delay === 0) return "ok";
  if (delay <= 15) return "mild";
  if (delay <= 60) return "medium";
  return "severe";
};

const delayLabel = (delay) => {
  if (delay > 0) return `+${delay} ${t("minutes")}`;
  return `${delay} ${t("minutes")}`;
};

const setStatus = (text, type = "") => {
  statusPillEl.textContent = text;
  statusPillEl.classList.remove("ok", "error");
  if (type) {
    statusPillEl.classList.add(type);
  }
};

const updateTextContent = (id, value) => {
  const element = document.querySelector(`#${id}`);
  if (element) {
    element.textContent = value;
  }
};

const markUserActivity = () => {
  state.lastUserActivityAtMs = Date.now();
};

const isUserInactive = () => Date.now() - state.lastUserActivityAtMs >= INACTIVITY_SYNC_MS;

const setPendingSync = (pending) => {
  state.hasPendingSync = pending;
  if (!syncBtnEl) {
    return;
  }

  syncBtnEl.classList.toggle("hidden", !pending);
};

const normalizeText = (value) => String(value ?? "").toLocaleLowerCase(locale());

const getTrainSortValue = (train, field) => {
  if (field === "train") return normalizeText(train.cod_comercial);
  if (field === "type") return normalizeText(train.product_name);
  if (field === "corridor") return normalizeText(train.des_corridor);
  if (field === "route")
    return normalizeText(
      stationPair(train.origin_name, train.destination_name, train.cod_origen, train.cod_destino),
    );
  if (field === "next")
    return normalizeText(nextStation(train.next_station_name, train.cod_est_sig, train.hora_llegada_sig_est));
  if (field === "delay") return Number(train.ult_retraso ?? 0);
  if (field === "last") return Number(train.last_seen_at ?? 0);
  return "";
};

const sortTrains = (items) => {
  const direction = state.sortDir === "asc" ? 1 : -1;
  const field = state.sortBy;

  return [...items].sort((a, b) => {
    const va = getTrainSortValue(a, field);
    const vb = getTrainSortValue(b, field);

    if (typeof va === "number" && typeof vb === "number") {
      return (va - vb) * direction;
    }

    return String(va).localeCompare(String(vb), locale()) * direction;
  });
};

const renderSortIndicators = () => {
  for (const button of sortButtons) {
    const field = button.dataset.sort;
    if (!field) {
      continue;
    }

    const indicator = document.querySelector(`#sort-indicator-${field}`);
    const isActive = field === state.sortBy;
    button.classList.toggle("active", isActive);

    if (indicator) {
      indicator.textContent = isActive ? (state.sortDir === "asc" ? "↑" : "↓") : "↕";
    }
  }
};

const setHistoryButtonsActive = () => {
  const hasCustom = Boolean(state.historyFrom || state.historyTo);
  for (const button of historyButtons) {
    const isActive = !hasCustom && Number(button.dataset.hours) === state.historyHours;
    button.classList.toggle("active", isActive);
  }
};

const applyStaticTexts = () => {
  document.documentElement.lang = state.lang;
  numberFmt = new Intl.NumberFormat(locale());

  updateTextContent("kicker-text", t("kicker"));
  updateTextContent("title-text", t("title"));
  updateTextContent("subtitle-text", t("subtitle"));
  updateTextContent("lang-label", t("langLabel"));
  updateTextContent("theme-label", t("themeLabel"));
  if (themeOptionSystemEl) {
    themeOptionSystemEl.textContent = t("themeSystem");
  }
  if (themeOptionLightEl) {
    themeOptionLightEl.textContent = t("themeLight");
  }
  if (themeOptionDarkEl) {
    themeOptionDarkEl.textContent = t("themeDark");
  }
  updateTextContent("api-docs-btn", t("apiDocs"));
  updateTextContent("raw-toggle-btn", state.isRawOpen ? t("rawToggleHide") : t("rawToggle"));
  updateTextContent("raw-title", t("rawTitle"));
  updateTextContent("copy-raw-btn", t("rawCopy"));
  updateTextContent("close-raw-btn", t("rawClose"));
  updateTextContent("recover-history-btn", t("recoverHistory"));
  updateTextContent("delay-title", t("delayTitle"));
  updateTextContent("products-title", t("productsTitle"));
  updateTextContent("corridors-title", t("corridorsTitle"));
  updateTextContent("today-title", t("todayTitle"));
  updateTextContent("historical-title", t("historicalTitle"));
  updateTextContent("historical-highlights-title", t("historicalHighlightsTitle"));
  updateTextContent("historical-products-title", t("historicalProductsTitle"));
  const accountabilityRoutesTitleEl = document.querySelector("#accountability-routes-title");
  if (accountabilityRoutesTitleEl) {
    accountabilityRoutesTitleEl.innerHTML = metricLabelWithInfo(
      t("accountabilityRoutesTitle"),
      t("tipCriticalRoutes"),
      "title-critical-routes",
    );
  }
  const accountabilityTrainsTitleEl = document.querySelector("#accountability-trains-title");
  if (accountabilityTrainsTitleEl) {
    accountabilityTrainsTitleEl.innerHTML = metricLabelWithInfo(
      t("accountabilityTrainsTitle"),
      t("tipRepeatOffenders"),
      "title-repeat-offenders",
    );
  }
  updateTextContent("history-from-label", t("historyFromLabel"));
  updateTextContent("history-to-label", t("historyToLabel"));
  updateTextContent("history-range-apply", t("historyApplyRange"));
  updateTextContent("history-range-clear", t("historyClearRange"));
  updateTextContent("trains-title", t("trainsTitle"));
  updateTextContent("apply-btn", t("apply"));
  updateTextContent("sync-btn", t("syncNow"));
  updateTextContent("export-csv-btn", t("exportCsv"));
  updateTextContent("export-excel-btn", t("exportExcel"));
  updateTextContent("th-train", t("thTrain"));
  updateTextContent("th-type", t("thType"));
  updateTextContent("th-corridor", t("thCorridor"));
  updateTextContent("th-route", t("thRoute"));
  updateTextContent("th-next", t("thNext"));
  updateTextContent("th-delay", t("thDelay"));
  updateTextContent("th-last", t("thLast"));

  searchInput.placeholder = t("searchPlaceholder");
  minDelayInput.placeholder = t("minDelayPlaceholder");
  if (corridorsSearchInputEl) {
    corridorsSearchInputEl.placeholder = t("corridorsSearchPlaceholder");
  }
  if (historicalProductsSearchInputEl) {
    historicalProductsSearchInputEl.placeholder = t("historicalProductsSearchPlaceholder");
  }
  if (accountabilityRoutesSearchInputEl) {
    accountabilityRoutesSearchInputEl.placeholder = t("accountabilityRoutesSearchPlaceholder");
  }
  if (accountabilityTrainsSearchInputEl) {
    accountabilityTrainsSearchInputEl.placeholder = t("accountabilityTrainsSearchPlaceholder");
  }

  if (!latestDashboard) {
    setStatus(t("statusLoading"));
  }

  setHistoryButtonsActive();
  historyFromInputEl.value = state.historyFrom;
  historyToInputEl.value = state.historyTo;
  renderSortIndicators();
};

const buildUrl = (path) => {
  return new URL(path, window.location.origin).toString();
};

const requestApiKey = async ({ force = false } = {}) => {
  if (!force && state.apiKey && Date.now() < state.apiKeyExpiresAt - 10_000) {
    return;
  }

  const response = await fetch(buildUrl("/api/auth/request-key"), {
    method: "GET",
    cache: "no-store",
    headers: {
      "accept-language": state.lang,
    },
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

const isAuthError = (payload) => {
  if (!payload || typeof payload !== "object") {
    return false;
  }

  return payload.code === "MISSING_API_KEY" || payload.code === "INVALID_API_KEY" || payload.code === "EXPIRED_API_KEY";
};

const apiFetch = async (path, options = {}) => {
  const execute = async () => {
    await requestApiKey();

    for (let attempt = 0; attempt < 3; attempt += 1) {
      const elapsed = Date.now() - state.lastRequestAtMs;
      const waitMs = Math.max(0, state.apiMinIntervalMs - elapsed);
      if (waitMs > 0) {
        await sleep(waitMs);
      }

      const headers = new Headers(options.headers || {});
      headers.set("accept-language", state.lang);
      headers.set("x-api-key", state.apiKey || "");

      const response = await fetch(buildUrl(path), {
        ...options,
        headers,
        cache: "no-store",
      });

      state.lastRequestAtMs = Date.now();

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
        await sleep(1100);
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
    const message = payload?.message || `${response.status}`;
    throw new Error(message);
  }

  return payload;
};

const renderOverviewCards = (overview, today, typeInsights) => {
  const problematic = typeInsights?.problematic ?? null;
  const volume = typeInsights?.volume ?? null;

  const problematicValue = problematic
    ? `${problematic.productName} · ${formatLargeInt(problematic.accumulatedDelayMinutes)} ${t("minutes")}`
    : t("noTypeData");

  const volumeValue = volume
    ? `${volume.productName} · ${numberFmt.format(volume.trains)} / ${numberFmt.format(Math.round(volume.totalKm))} km`
    : t("noTypeData");

  const cards = [
    { label: t("cardActive"), value: numberFmt.format(overview.activeTrains) },
    { label: t("cardAvg"), value: `${overview.avgDelay.toFixed(1)} ${t("minutes")}` },
    { label: t("cardMax"), value: `${overview.maxDelay} ${t("minutes")}` },
    { label: t("cardOver15"), value: numberFmt.format(overview.delayedOver15) },
    { label: t("cardOver60"), value: numberFmt.format(overview.severeOver60) },
    { label: t("cardAccessible"), value: `${overview.accessiblePct}%` },
    { label: t("cardObs"), value: numberFmt.format(today.observations) },
    { label: t("cardKm"), value: numberFmt.format(Math.round(today.kmTracked)) },
    { label: t("cardProblematicType"), value: problematicValue },
    { label: t("cardVolumeType"), value: volumeValue },
  ];

  overviewCardsEl.innerHTML = cards
    .map(
      (card) => `
      <article class="card">
        <div class="label">${escapeHtml(card.label)}</div>
        <div class="value">${escapeHtml(card.value)}</div>
      </article>
    `,
    )
    .join("");
};

const renderBarRows = (target, rows) => {
  target.classList.remove("skeleton-bars");
  target.innerHTML = rows
    .map((row) => {
      const pct = Math.max(0, Math.min(100, row.pct));
      return `
        <div class="bar-row">
          <span>${escapeHtml(row.label)}</span>
          <div class="track"><div class="fill ${row.tone ?? ""}" style="width: ${pct.toFixed(1)}%"></div></div>
          <strong>${escapeHtml(row.value)}</strong>
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
  ].map((row) => ({
    ...row,
    pct: (row.value / total) * 100,
    value: numberFmt.format(row.value),
  }));

  renderBarRows(delayBucketsEl, rows);
};

const renderProducts = (products) => {
  const max = products.reduce((acc, item) => Math.max(acc, item.count), 1);

  const rows = products.slice(0, 10).map((item) => ({
    label: `${item.productName} (${item.codProduct})`,
    value: `${item.count} | ${item.avgDelay.toFixed(1)} ${t("minutes")}`,
    pct: (item.count / max) * 100,
    tone: item.avgDelay > 60 ? "danger" : item.avgDelay > 15 ? "warning" : "",
  }));

  renderBarRows(productsEl, rows);
};

const renderCorridors = (corridors) => {
  const filter = state.corridorsQuery.trim().toLocaleLowerCase(locale());
  const visibleCorridors =
    filter.length === 0
      ? corridors
      : corridors.filter((item) =>
          `${String(item.corridor || "")} ${String(item.axisLabel || "")} ${String(item.axisKey || "")}`
            .toLocaleLowerCase(locale())
            .includes(filter),
        );

  if (!Array.isArray(visibleCorridors) || visibleCorridors.length === 0) {
    corridorsEl.innerHTML = `<div class="list-item"><div class="meta">${escapeHtml(t("noLocalRows"))}</div></div>`;
    return;
  }

  corridorsEl.innerHTML = visibleCorridors
    .map(
      (item) => {
        const sourceLabel =
          item.source === "derived_axis" ? t("corridorSourceDerived") : t("corridorSourceOfficial");
        const sourceClass = item.source === "derived_axis" ? "derived" : "official";
        return `
      <div class="list-item">
        <div class="name">${escapeHtml(item.corridor)} <span class="source-badge ${sourceClass}">${escapeHtml(sourceLabel)}</span></div>
        <div class="meta">${numberFmt.format(item.train_count)} ${t("corridorActive")}</div>
        <div class="meta">${t("corridorDelay")}: ${item.avg_delay.toFixed(1)} ${t("minutes")} | ${t("corridorPeak")}: ${item.max_delay} ${t("minutes")}</div>
      </div>
    `;
      },
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
      label: t("todayWeighted"),
      labelHtml: metricLabelWithInfo(t("todayWeighted"), t("tipWeightedAvgDelay"), "today-weighted-delay"),
      value: `${today.weightedAvgDelay.toFixed(1)} ${t("minutes")}`,
    },
    { label: t("todayPeak"), value: `${today.peakDelay} ${t("minutes")}` },
    { label: t("todayKm"), value: `${Math.round(today.kmTracked)} km` },
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

const renderAccountability = (accountability) => {
  accountabilityRoutesEl.classList.remove("skeleton-list");
  accountabilityTrainsEl.classList.remove("skeleton-list");

  if (!accountability || !accountability.summary) {
    accountabilityRoutesEl.innerHTML = `<div class="list-item"><div class="meta">${escapeHtml(t("accountabilityNoData"))}</div></div>`;
    accountabilityTrainsEl.innerHTML = `<div class="list-item"><div class="meta">${escapeHtml(t("accountabilityNoData"))}</div></div>`;
    return;
  }

  const summary = accountability.summary || {};
  const worsening = summary.worseningVs7d || {};
  const trend =
    worsening.trend === "worsening"
      ? t("accountabilityTrendWorsening")
      : worsening.trend === "improving"
        ? t("accountabilityTrendImproving")
        : t("accountabilityTrendFlat");

  const routes = Array.isArray(accountability.criticalRoutes) ? accountability.criticalRoutes : [];
  const routesFilter = state.accountabilityRoutesQuery.trim().toLocaleLowerCase(locale());
  const visibleRoutes =
    routesFilter.length === 0
      ? routes
      : routes.filter((item) =>
          `${String(item.axisLabel || "")} ${String(item.axisKey || "")}`
            .toLocaleLowerCase(locale())
            .includes(routesFilter),
        );

  const routesSummary = `
    <div class="list-item compact">
      <div class="name">${metricLabelWithInfo(t("accountabilityDelayed15"), t("tipDelayed15"), "accountability-delayed15")} ${Number(summary.delayed15Pct || 0).toFixed(1)}%</div>
      <div class="meta">${metricLabelWithInfo(t("accountabilitySevere60"), t("tipSevere60"), "accountability-severe60")} ${Number(summary.severe60Pct || 0).toFixed(1)}%</div>
      <div class="meta">${escapeHtml(t("accountabilityTrendLabel"))}: ${escapeHtml(trend)}</div>
    </div>
  `;

  const routeRows = visibleRoutes
    .slice(0, 12)
    .map((item) => {
      const sourceLabel =
        item.source === "derived_axis" ? t("corridorSourceDerived") : t("corridorSourceOfficial");
      const sourceClass = item.source === "derived_axis" ? "derived" : "official";
      return `
        <div class="list-item">
          <div class="name">${escapeHtml(item.axisLabel || "-")} <span class="source-badge ${sourceClass}">${escapeHtml(sourceLabel)}</span></div>
          <div class="meta">${numberFmt.format(item.observations || 0)} ${t("histObsShort")} · ${Number(item.avgDelay || 0).toFixed(1)} ${t("minutes")} · max ${item.maxDelay || 0} ${t("minutes")}</div>
          <div class="meta">${t("accountabilityDelayed15")}: ${Number(item.delayed15Pct || 0).toFixed(1)}% · ${t("accountabilitySevere60")}: ${Number(item.severe60Pct || 0).toFixed(1)}%</div>
        </div>
      `;
    })
    .join("");

  accountabilityRoutesEl.innerHTML =
    routesSummary +
    (routeRows.length > 0
      ? routeRows
      : `<div class="list-item"><div class="meta">${escapeHtml(t("noLocalRows"))}</div></div>`);

  const offenders = Array.isArray(accountability.repeatOffenders) ? accountability.repeatOffenders : [];
  const offendersFilter = state.accountabilityTrainsQuery.trim().toLocaleLowerCase(locale());
  const visibleOffenders =
    offendersFilter.length === 0
      ? offenders
      : offenders.filter((item) =>
          `${String(item.codComercial || "")} ${String(item.productName || item.codProduct || "")} ${String(item.origin || "")} ${String(item.destination || "")}`
            .toLocaleLowerCase(locale())
            .includes(offendersFilter),
        );

  if (visibleOffenders.length === 0) {
    accountabilityTrainsEl.innerHTML = `<div class="list-item"><div class="meta">${escapeHtml(t("noLocalRows"))}</div></div>`;
    return;
  }

  accountabilityTrainsEl.innerHTML = visibleOffenders
    .slice(0, 12)
    .map(
      (item) => `
        <div class="list-item">
          <div class="name">${escapeHtml(item.codComercial || "-")} · ${escapeHtml(item.productName || item.codProduct || "-")}</div>
          <div class="meta">${escapeHtml(stationPair(item.origin, item.destination, null, null))}</div>
          <div class="meta">${numberFmt.format(item.observations || 0)} ${t("histObsShort")} · ${Number(item.avgDelay || 0).toFixed(1)} ${t("minutes")} · max ${item.maxDelay || 0} ${t("minutes")}</div>
          <div class="meta">${t("accountabilityDelayed15")}: ${Number(item.delayed15Pct || 0).toFixed(1)}% · ${t("accountabilitySevere60")}: ${Number(item.severe60Pct || 0).toFixed(1)}%</div>
        </div>
      `,
    )
    .join("");
};

const renderHistorical = (historical) => {
  historicalProductsEl.classList.remove("skeleton-bars");
  historicalHighlightsEl.classList.remove("skeleton-list");
  accountabilityRoutesEl.classList.remove("skeleton-list");
  accountabilityTrainsEl.classList.remove("skeleton-list");
  if (!historical || !historical.summary) {
    historicalCardsEl.innerHTML = "";
    historicalHighlightsEl.innerHTML = `<div class="list-item"><div class="meta">${escapeHtml(t("histNoData"))}</div></div>`;
    historicalProductsEl.innerHTML = "";
    accountabilityRoutesEl.innerHTML = `<div class="list-item"><div class="meta">${escapeHtml(t("accountabilityNoData"))}</div></div>`;
    accountabilityTrainsEl.innerHTML = `<div class="list-item"><div class="meta">${escapeHtml(t("accountabilityNoData"))}</div></div>`;
    return;
  }

  const summary = historical.summary;
  const ingestion = historical.ingestion || {};

  const cards = [
    { label: t("histCardObservations"), value: numberFmt.format(summary.observations || 0) },
    { label: t("histCardUniqueTrains"), value: numberFmt.format(summary.uniqueTrains || 0) },
    { label: t("histCardAvgDelay"), value: `${Number(summary.avgDelay || 0).toFixed(1)} ${t("minutes")}` },
    { label: t("histCardMaxDelay"), value: `${summary.maxDelay || 0} ${t("minutes")}` },
    {
      label: t("histCardAccumDelay"),
      labelHtml: metricLabelWithInfo(t("histCardAccumDelay"), t("tipAccumDelay"), "hist-accum-delay"),
      value: `${formatLargeInt(summary.accumulatedDelayMinutes)} ${t("minutes")}`,
    },
    { label: t("histCardOnTimePct"), value: `${Number(summary.onTimePct || 0).toFixed(1)}%` },
    { label: t("histCardSeverePct"), value: `${Number(summary.severePct || 0).toFixed(1)}%` },
    {
      label: t("histCardAvgBatch"),
      labelHtml: metricLabelWithInfo(t("histCardAvgBatch"), t("tipAvgTrainsPerBatch"), "hist-avg-batch"),
      value: numberFmt.format(Math.round(ingestion.avgTrainsPerBatch || 0)),
    },
  ];

  historicalCardsEl.innerHTML = cards
    .map(
      (card) => `
      <article class="card">
        <div class="label">${card.labelHtml ?? escapeHtml(card.label)}</div>
        <div class="value">${escapeHtml(card.value)}</div>
      </article>
    `,
    )
    .join("");

  const topType = historical.topProblematicProduct;
  const topCorridor = historical.topProblematicCorridor;

  const highlightRows = [];

  if (topType) {
    highlightRows.push(`
      <div class="list-item">
        <div class="name">${escapeHtml(t("histTopType"))}: ${escapeHtml(topType.productName || "-")}</div>
        <div class="meta">${formatLargeInt(topType.accumulatedDelayMinutes)} ${t("minutes")} · ${numberFmt.format(topType.observations || 0)} ${t("histObsShort")} · ${numberFmt.format(topType.trains || 0)} ${t("histTrainsShort")}</div>
      </div>
    `);
  }

  if (topCorridor) {
    const corridorName = topCorridor.corridor || t("histNoCorridor");
    const sourceLabel =
      topCorridor.source === "derived_axis" ? t("corridorSourceDerived") : t("corridorSourceOfficial");
    const sourceClass = topCorridor.source === "derived_axis" ? "derived" : "official";
    highlightRows.push(`
      <div class="list-item">
        <div class="name">${escapeHtml(t("histTopCorridor"))}: ${escapeHtml(corridorName)} <span class="source-badge ${sourceClass}">${escapeHtml(sourceLabel)}</span></div>
        <div class="meta">${formatLargeInt(topCorridor.accumulatedDelayMinutes)} ${t("minutes")} · ${numberFmt.format(topCorridor.observations || 0)} ${t("histObsShort")} · ${numberFmt.format(topCorridor.trains || 0)} ${t("histTrainsShort")}</div>
      </div>
    `);
  }

  historicalHighlightsEl.innerHTML =
    highlightRows.length > 0
      ? highlightRows.join("")
      : `<div class="list-item"><div class="meta">${escapeHtml(t("histNoData"))}</div></div>`;

  const byProduct = Array.isArray(historical.byProduct) ? historical.byProduct : [];
  const productFilter = state.historicalProductsQuery.trim().toLocaleLowerCase(locale());
  const visibleByProduct =
    productFilter.length === 0
      ? byProduct
      : byProduct.filter((item) => {
          const productName = item.productName || item.productNameEs || item.codProduct || item.cod_product;
          return String(productName ?? "")
            .toLocaleLowerCase(locale())
            .includes(productFilter);
        });

  if (visibleByProduct.length === 0) {
    historicalProductsEl.innerHTML = `<div class="list-item"><div class="meta">${escapeHtml(t("noLocalRows"))}</div></div>`;
    renderAccountability(historical.accountability ?? null);
    return;
  }

  const maxObs = visibleByProduct.reduce((max, item) => Math.max(max, Number(item.observations || 0)), 1);
  const rows = visibleByProduct.slice(0, 8).map((item) => ({
    label: `${item.productName || item.productNameEs || item.codProduct || "-"} (${item.codProduct ?? item.cod_product ?? "-"})`,
    value: `${numberFmt.format(item.observations || 0)} ${t("histObsShort")} | ${Number(item.avg_delay ?? item.avgDelay ?? 0).toFixed(1)} ${t("minutes")}`,
    pct: ((item.observations || 0) / maxObs) * 100,
    tone:
      Number(item.avg_delay ?? item.avgDelay ?? 0) > 60
        ? "danger"
        : Number(item.avg_delay ?? item.avgDelay ?? 0) > 15
          ? "warning"
          : "",
  }));

  renderBarRows(historicalProductsEl, rows);
  renderAccountability(historical.accountability ?? null);
};

const stationPair = (origin, destination, originCode, destinationCode) => {
  const from = origin || originCode || "-";
  const to = destination || destinationCode || "-";
  return `${from}${t("routeSeparator")}${to}`;
};

const nextStation = (nextName, nextCode, eta) => {
  const station = nextName || nextCode || "-";
  if (!eta) {
    return station;
  }

  const shortEta = eta.replace("T", " ").slice(0, 16);
  return `${station} (${shortEta})`;
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
      const css = delayClass(delay);
      const route = stationPair(
        train.origin_name,
        train.destination_name,
        train.cod_origen,
        train.cod_destino,
      );
      const next = nextStation(train.next_station_name, train.cod_est_sig, train.hora_llegada_sig_est);

      return `
        <tr>
          <td>${escapeHtml(train.cod_comercial)}</td>
          <td>${escapeHtml(train.product_name)}</td>
          <td>${escapeHtml(train.des_corridor || "-")}</td>
          <td>${escapeHtml(route)}</td>
          <td>${escapeHtml(next)}</td>
          <td><span class="delay ${css}">${escapeHtml(delayLabel(delay))}</span></td>
          <td>${escapeHtml(asTime(train.last_seen_at))}</td>
        </tr>
      `;
    })
    .join("");
};

const rerenderLocalFilteredPanels = () => {
  if (!latestDashboard) {
    return;
  }

  renderCorridors(Array.isArray(latestDashboard.topCorridors) ? latestDashboard.topCorridors : []);
  renderHistorical(latestDashboard.historical ?? null);
};

const renderRaw = (payload) => {
  if (!payload) {
    rawMetaEl.textContent = "-";
    rawJsonEl.textContent = "{ }";
    return;
  }

  const generatedAt = payload.generatedAt
    ? new Date(payload.generatedAt).toLocaleString(locale(), { hour12: false })
    : "-";

  const count = Array.isArray(payload.trainsCurrent) ? payload.trainsCurrent.length : 0;
  const coverage = payload.historyCoverage;
  const missing = coverage?.estimatedMissingRuns ?? 0;
  const observedRuns = coverage?.observedRuns ?? 0;
  const expectedRuns = coverage?.expectedRuns ?? 0;

  rawMetaEl.textContent = `${t("rawGenerated")}: ${generatedAt} · ${t("rawCount")}: ${count} · ${t("rawCoverage")}: ${observedRuns}/${expectedRuns} · ${t("rawMissingRuns")}: ${missing}`;
  rawJsonEl.textContent = JSON.stringify(payload, null, 2);
};

const loadDashboard = async () => {
  const params = new URLSearchParams();
  params.set("historyHours", String(state.historyHours));

  if (state.historyFrom) {
    params.set("historyFrom", state.historyFrom);
  }

  if (state.historyTo) {
    params.set("historyTo", state.historyTo);
  }

  const response = await apiFetch(`/api/dashboard?${params.toString()}`);
  const data = await readJsonResponse(response);

  latestDashboard = data;
  state.lastAppliedSuccessAt = Number(data?.ingestor?.lastSuccessAt || state.lastAppliedSuccessAt || 0);
  setPendingSync(false);

  renderOverviewCards(data.overview, data.today, data.typeInsights);
  overviewCardsEl.setAttribute("aria-busy", "false");
  renderDelayBuckets(data.delayBuckets);
  renderProducts(data.byProduct);
  renderCorridors(data.topCorridors);
  renderToday(data.today);
  renderHistorical(data.historical);

  lastSeenEl.textContent = data.overview.lastSeenAtIso
    ? `${t("lastSignal")}: ${new Date(data.overview.lastSeenAtIso).toLocaleString(locale(), {
        hour12: false,
      })}`
    : `${t("lastSignal")}: -`;

  const ingest = data.ingestor;
  if (ingest.lastError) {
    setStatus(`${t("statusError")}: ${ingest.lastError}`, "error");
  } else {
    const updatedAt =
      (ingest.lastSuccessAt ? new Date(ingest.lastSuccessAt * 1000) : null) ??
      (data.overview.lastSeenAtIso ? new Date(data.overview.lastSeenAtIso) : null);

    if (updatedAt) {
      setStatus(`${t("statusLastUpdate")}: ${updatedAt.toLocaleString(locale(), { hour12: false })}`, "ok");
    } else {
      setStatus(t("statusNoUpdate"), "ok");
    }
  }
};

const loadTrains = async () => {
  const params = new URLSearchParams();
  params.set("limit", String(state.limit));
  params.set("offset", String(state.offset));

  if (state.query.trim()) {
    params.set("q", state.query.trim());
  }

  if (state.minDelay.trim()) {
    params.set("minDelay", state.minDelay.trim());
  }

  const response = await apiFetch(`/api/trains?${params.toString()}`);
  const data = await readJsonResponse(response);
  renderTrains(data);
};

const loadRawLive = async () => {
  if (!state.isRawOpen) {
    return;
  }

  try {
    const response = await apiFetch("/api/raw/live");
    const data = await readJsonResponse(response);
    renderRaw(data);
  } catch (error) {
    rawMetaEl.textContent = error instanceof Error ? error.message : String(error);
  }
};

const checkForPendingSync = async () => {
  try {
    const response = await fetch(buildUrl("/api/health"), {
      method: "GET",
      cache: "no-store",
      headers: {
        "accept-language": state.lang,
      },
    });
    const data = await readJsonResponse(response);
    const incomingSuccessAt = Number(data?.ingestor?.lastSuccessAt || 0);
    if (incomingSuccessAt > state.lastAppliedSuccessAt) {
      setPendingSync(true);
      setStatus(t("syncAvailable"), "ok");
    }
  } catch {
    // noop: non-blocking polling
  }
};

const refresh = async () => {
  try {
    await loadDashboard();
    await loadTrains();
    if (state.isRawOpen) {
      await loadRawLive();
    }
  } catch (error) {
    setStatus(error instanceof Error ? error.message : String(error), "error");
  }
};

const refreshFirstPaint = async () => {
  try {
    await loadDashboard();
    setTimeout(() => {
      void loadTrains().catch((error) => {
        setStatus(error instanceof Error ? error.message : String(error), "error");
      });
    }, 0);
  } catch (error) {
    setStatus(error instanceof Error ? error.message : String(error), "error");
  }
};

const autoRefreshTick = async () => {
  if (isUserInactive()) {
    await refresh();
    return;
  }

  await checkForPendingSync();
};

const stopRawPolling = () => {
  if (state.rawTimer) {
    clearInterval(state.rawTimer);
    state.rawTimer = null;
  }
};

const startRawPolling = () => {
  stopRawPolling();
  void loadRawLive();
  state.rawTimer = setInterval(() => {
    void loadRawLive();
  }, 3000);
};

const toggleRawPanel = () => {
  state.isRawOpen = !state.isRawOpen;
  rawPanelEl.classList.toggle("hidden", !state.isRawOpen);
  applyStaticTexts();

  if (state.isRawOpen) {
    startRawPolling();
  } else {
    stopRawPolling();
  }
};

const toCsvCell = (value) => `"${String(value ?? "").replaceAll('"', '""')}"`;

const toExportRows = (items) => {
  return sortTrains(items).map((train) => {
    const delay = Number(train.ult_retraso ?? 0);
    return {
      train: train.cod_comercial ?? "",
      type: train.product_name ?? "",
      corridor: train.des_corridor ?? "",
      route: stationPair(train.origin_name, train.destination_name, train.cod_origen, train.cod_destino),
      next: nextStation(train.next_station_name, train.cod_est_sig, train.hora_llegada_sig_est),
      delay,
      last: asTime(train.last_seen_at),
    };
  });
};

const fetchAllFilteredTrains = async () => {
  const items = [];
  const limit = 200;
  let offset = 0;
  let total = Infinity;

  while (offset < total) {
    const params = new URLSearchParams();
    params.set("limit", String(limit));
    params.set("offset", String(offset));

    if (state.query.trim()) {
      params.set("q", state.query.trim());
    }

    if (state.minDelay.trim()) {
      params.set("minDelay", state.minDelay.trim());
    }

    const response = await apiFetch(`/api/trains?${params.toString()}`);
    const data = await readJsonResponse(response);
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
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
};

filtersForm.addEventListener("submit", (event) => {
  event.preventDefault();
  markUserActivity();

  state.query = searchInput.value;
  state.minDelay = minDelayInput.value;
  state.offset = 0;

  void loadTrains();
});

langSwitchEl.addEventListener("change", () => {
  markUserActivity();
  state.lang = langSwitchEl.value === "en" ? "en" : "es";
  localStorage.setItem("retrasometro_lang", state.lang);
  localStorage.removeItem("renfe_lang");
  applyStaticTexts();
  void refresh();
});

for (const button of historyButtons) {
  button.addEventListener("click", () => {
    markUserActivity();
    const hours = Number(button.dataset.hours);
    if (!Number.isFinite(hours) || hours < 1) {
      return;
    }

    state.historyHours = hours;
    state.historyFrom = "";
    state.historyTo = "";
    localStorage.setItem("retrasometro_history_hours", String(hours));
    localStorage.removeItem("retrasometro_history_from");
    localStorage.removeItem("retrasometro_history_to");
    localStorage.removeItem("renfe_history_hours");
    localStorage.removeItem("renfe_history_from");
    localStorage.removeItem("renfe_history_to");
    setHistoryButtonsActive();
    historyFromInputEl.value = "";
    historyToInputEl.value = "";
    void refresh();
  });
}

historyRangeApplyBtnEl.addEventListener("click", () => {
  markUserActivity();
  state.historyFrom = historyFromInputEl.value || "";
  state.historyTo = historyToInputEl.value || "";
  localStorage.setItem("retrasometro_history_from", state.historyFrom);
  localStorage.setItem("retrasometro_history_to", state.historyTo);
  localStorage.removeItem("renfe_history_from");
  localStorage.removeItem("renfe_history_to");
  setHistoryButtonsActive();
  setStatus(t("histRangeApplied"), "ok");
  void refresh();
});

historyRangeClearBtnEl.addEventListener("click", () => {
  markUserActivity();
  state.historyFrom = "";
  state.historyTo = "";
  localStorage.removeItem("retrasometro_history_from");
  localStorage.removeItem("retrasometro_history_to");
  localStorage.removeItem("renfe_history_from");
  localStorage.removeItem("renfe_history_to");
  historyFromInputEl.value = "";
  historyToInputEl.value = "";
  setHistoryButtonsActive();
  setStatus(t("histRangeCleared"), "ok");
  void refresh();
});

apiDocsBtnEl.addEventListener("click", () => {
  markUserActivity();
  const docsUrl = buildUrl("/api-docs.html");
  const opened = window.open(docsUrl, "_blank", "noopener,noreferrer");
  if (!opened) {
    setStatus(t("docsOpenFail"), "error");
  }
});

if (rawToggleBtnEl) {
  rawToggleBtnEl.addEventListener("click", () => {
    markUserActivity();
    toggleRawPanel();
  });
}

closeRawBtnEl.addEventListener("click", () => {
  markUserActivity();
  if (state.isRawOpen) {
    toggleRawPanel();
  }
});

copyRawBtnEl.addEventListener("click", async () => {
  markUserActivity();
  try {
    await navigator.clipboard.writeText(buildUrl("/api/raw/live"));
    setStatus(t("rawCopyOk"), "ok");
  } catch {
    setStatus(t("rawCopyFail"), "error");
  }
});

recoverHistoryBtnEl.addEventListener("click", async () => {
  markUserActivity();
  try {
    const response = await apiFetch("/api/history/recover?hours=48", { method: "POST" });
    const data = await readJsonResponse(response);
    setStatus(`${t("recoverDone")} · +${data.recovered}`, "ok");
    await loadRawLive();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    setStatus(`${t("recoverFail")}: ${message}`, "error");
  }
});

if (syncBtnEl) {
  syncBtnEl.addEventListener("click", async () => {
    markUserActivity();
    await refresh();
  });
}

for (const button of sortButtons) {
  button.addEventListener("click", () => {
    markUserActivity();
    const field = button.dataset.sort;
    if (!field) {
      return;
    }

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

searchInput.addEventListener("input", markUserActivity);
minDelayInput.addEventListener("input", markUserActivity);
if (themeSwitchEl) {
  themeSwitchEl.addEventListener("change", markUserActivity);
}

if (corridorsSearchInputEl) {
  corridorsSearchInputEl.addEventListener("input", () => {
    markUserActivity();
    state.corridorsQuery = corridorsSearchInputEl.value || "";
    rerenderLocalFilteredPanels();
  });
}

if (historicalProductsSearchInputEl) {
  historicalProductsSearchInputEl.addEventListener("input", () => {
    markUserActivity();
    state.historicalProductsQuery = historicalProductsSearchInputEl.value || "";
    rerenderLocalFilteredPanels();
  });
}

if (accountabilityRoutesSearchInputEl) {
  accountabilityRoutesSearchInputEl.addEventListener("input", () => {
    markUserActivity();
    state.accountabilityRoutesQuery = accountabilityRoutesSearchInputEl.value || "";
    rerenderLocalFilteredPanels();
  });
}

if (accountabilityTrainsSearchInputEl) {
  accountabilityTrainsSearchInputEl.addEventListener("input", () => {
    markUserActivity();
    state.accountabilityTrainsQuery = accountabilityTrainsSearchInputEl.value || "";
    rerenderLocalFilteredPanels();
  });
}

if (exportCsvBtnEl) {
  exportCsvBtnEl.addEventListener("click", async () => {
    markUserActivity();
    try {
      const rows = toExportRows(await fetchAllFilteredTrains());
      const header = [t("thTrain"), t("thType"), t("thCorridor"), t("thRoute"), t("thNext"), t("thDelay"), t("thLast")];
      const body = rows.map((row) =>
        [
          toCsvCell(row.train),
          toCsvCell(row.type),
          toCsvCell(row.corridor),
          toCsvCell(row.route),
          toCsvCell(row.next),
          toCsvCell(row.delay),
          toCsvCell(row.last),
        ].join(";"),
      );
      const csv = `\uFEFF${header.map(toCsvCell).join(";")}\n${body.join("\n")}\n`;
      downloadFile(`trenes-activos-${new Date().toISOString().slice(0, 19).replaceAll(":", "-")}.csv`, "text/csv;charset=utf-8", csv);
      setStatus(t("exportDone"), "ok");
    } catch (error) {
      setStatus(`${t("exportFail")}: ${error instanceof Error ? error.message : String(error)}`, "error");
    }
  });
}

if (exportExcelBtnEl) {
  exportExcelBtnEl.addEventListener("click", async () => {
    markUserActivity();
    try {
      const rows = toExportRows(await fetchAllFilteredTrains());
      const htmlRows = rows
        .map(
          (row) =>
            `<tr><td>${escapeHtml(row.train)}</td><td>${escapeHtml(row.type)}</td><td>${escapeHtml(row.corridor)}</td><td>${escapeHtml(row.route)}</td><td>${escapeHtml(row.next)}</td><td>${escapeHtml(row.delay)}</td><td>${escapeHtml(row.last)}</td></tr>`,
        )
        .join("");
      const html = `<!doctype html><html><head><meta charset="utf-8" /></head><body><table border="1"><thead><tr><th>${escapeHtml(t("thTrain"))}</th><th>${escapeHtml(t("thType"))}</th><th>${escapeHtml(t("thCorridor"))}</th><th>${escapeHtml(t("thRoute"))}</th><th>${escapeHtml(t("thNext"))}</th><th>${escapeHtml(t("thDelay"))}</th><th>${escapeHtml(t("thLast"))}</th></tr></thead><tbody>${htmlRows}</tbody></table></body></html>`;
      downloadFile(
        `trenes-activos-${new Date().toISOString().slice(0, 19).replaceAll(":", "-")}.xls`,
        "application/vnd.ms-excel;charset=utf-8",
        `\uFEFF${html}`,
      );
      setStatus(t("exportDone"), "ok");
    } catch (error) {
      setStatus(`${t("exportFail")}: ${error instanceof Error ? error.message : String(error)}`, "error");
    }
  });
}

document.addEventListener("click", (event) => {
  if (!(event.target instanceof Element)) {
    return;
  }

  const trigger = event.target.closest(".info-trigger");
  if (trigger) {
    const wrapper = trigger.closest(".metric-info");
    if (!wrapper) {
      return;
    }

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
  if (themeSwitchEl) {
    themeController = initThemeController({
      selectEl: themeSwitchEl,
    });
    themeSwitchEl.value = themeController.getMode();
  }

  langSwitchEl.value = state.lang;
  localStorage.removeItem("retrasometro_history_from");
  localStorage.removeItem("retrasometro_history_to");
  localStorage.removeItem("renfe_history_from");
  localStorage.removeItem("renfe_history_to");
  applyStaticTexts();
  setStatus(t("statusLoading"));

  try {
    await refreshFirstPaint();
  } catch (error) {
    setStatus(error instanceof Error ? error.message : String(error), "error");
  }

  const activityEvents = ["pointerdown", "keydown", "mousemove", "touchstart", "scroll"];
  for (const eventName of activityEvents) {
    window.addEventListener(eventName, markUserActivity, { passive: true });
  }

  setInterval(() => {
    void autoRefreshTick();
  }, AUTO_REFRESH_MS);
};

void boot();
