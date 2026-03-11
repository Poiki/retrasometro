const overviewCardsEl = document.querySelector("#overview-cards");
const delayBucketsEl = document.querySelector("#delay-buckets");
const productsEl = document.querySelector("#products");
const corridorsEl = document.querySelector("#corridors");
const todayMetricsEl = document.querySelector("#today-metrics");
const trainsBodyEl = document.querySelector("#trains-body");
const statusPillEl = document.querySelector("#status-pill");
const lastSeenEl = document.querySelector("#last-seen");
const historicalCardsEl = document.querySelector("#historical-cards");
const historicalHighlightsEl = document.querySelector("#historical-highlights");
const historicalProductsEl = document.querySelector("#historical-products");
const historyButtons = [...document.querySelectorAll(".history-btn")];
const historyFromInputEl = document.querySelector("#history-from");
const historyToInputEl = document.querySelector("#history-to");
const historyRangeApplyBtnEl = document.querySelector("#history-range-apply");
const historyRangeClearBtnEl = document.querySelector("#history-range-clear");

const filtersForm = document.querySelector("#filters");
const searchInput = document.querySelector("#search");
const minDelayInput = document.querySelector("#min-delay");

const langSwitchEl = document.querySelector("#lang-switch");
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
    title: "Panel operativo de retrasos y actividad",
    subtitle: "Ingesta cada minuto, snapshots compactados y métricas en tiempo real.",
    langLabel: "Idioma",
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
    histRangeApplied: "Rango histórico aplicado",
    histRangeCleared: "Rango histórico limpiado",
    histObsShort: "obs",
    histTrainsShort: "trenes",
    bucketAhead: "Adelantados",
    bucketOnTime: "En hora",
    bucketMild: "1-15 min",
    bucketMedium: "16-60 min",
    bucketSevere: "> 60 min",
    corridorActive: "trenes activos",
    corridorDelay: "Retraso medio",
    corridorPeak: "pico",
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
    minutes: "min",
    docsOpenFail: "No se pudo abrir la documentación API",
  },
  en: {
    kicker: "retrasometro",
    title: "Operations dashboard for delays and activity",
    subtitle: "One-minute ingestion, compacted snapshots, and live metrics.",
    langLabel: "Language",
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
    histRangeApplied: "Historical range applied",
    histRangeCleared: "Historical range cleared",
    histObsShort: "obs",
    histTrainsShort: "trains",
    bucketAhead: "Ahead",
    bucketOnTime: "On time",
    bucketMild: "1-15 min",
    bucketMedium: "16-60 min",
    bucketSevere: "> 60 min",
    corridorActive: "active trains",
    corridorDelay: "Average delay",
    corridorPeak: "peak",
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
  historyFrom: storageGet("retrasometro_history_from", "renfe_history_from") || "",
  historyTo: storageGet("retrasometro_history_to", "renfe_history_to") || "",
  apiKey: null,
  apiKeyExpiresAt: 0,
  requestQueue: Promise.resolve(),
  lastRequestAtMs: 0,
  isRawOpen: false,
  rawTimer: null,
};

let latestDashboard = null;
let numberFmt = new Intl.NumberFormat("es-ES");

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
  updateTextContent("history-from-label", t("historyFromLabel"));
  updateTextContent("history-to-label", t("historyToLabel"));
  updateTextContent("history-range-apply", t("historyApplyRange"));
  updateTextContent("history-range-clear", t("historyClearRange"));
  updateTextContent("trains-title", t("trainsTitle"));
  updateTextContent("apply-btn", t("apply"));
  updateTextContent("th-train", t("thTrain"));
  updateTextContent("th-type", t("thType"));
  updateTextContent("th-corridor", t("thCorridor"));
  updateTextContent("th-route", t("thRoute"));
  updateTextContent("th-next", t("thNext"));
  updateTextContent("th-delay", t("thDelay"));
  updateTextContent("th-last", t("thLast"));

  searchInput.placeholder = t("searchPlaceholder");
  minDelayInput.placeholder = t("minDelayPlaceholder");

  if (!latestDashboard) {
    setStatus(t("statusLoading"));
  }

  setHistoryButtonsActive();
  historyFromInputEl.value = state.historyFrom;
  historyToInputEl.value = state.historyTo;
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
      const waitMs = Math.max(0, 1000 - elapsed);
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
    ? `${problematic.productName} · ${numberFmt.format(Math.round(problematic.accumulatedDelayMinutes))} ${t("minutes")}`
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
  corridorsEl.innerHTML = corridors
    .map(
      (item) => `
      <div class="list-item">
        <div class="name">${escapeHtml(item.corridor)}</div>
        <div class="meta">${numberFmt.format(item.train_count)} ${t("corridorActive")}</div>
        <div class="meta">${t("corridorDelay")}: ${item.avg_delay.toFixed(1)} ${t("minutes")} | ${t("corridorPeak")}: ${item.max_delay} ${t("minutes")}</div>
      </div>
    `,
    )
    .join("");
};

const renderToday = (today) => {
  const items = [
    { label: t("todayDate"), value: today.day },
    { label: t("todayUnique"), value: numberFmt.format(today.uniqueTrains) },
    { label: t("todayObs"), value: numberFmt.format(today.observations) },
    { label: t("todayWeighted"), value: `${today.weightedAvgDelay.toFixed(1)} ${t("minutes")}` },
    { label: t("todayPeak"), value: `${today.peakDelay} ${t("minutes")}` },
    { label: t("todayKm"), value: `${Math.round(today.kmTracked)} km` },
  ];

  todayMetricsEl.innerHTML = items
    .map(
      (item) => `
      <div class="today-box">
        <div class="label">${escapeHtml(item.label)}</div>
        <div class="value">${escapeHtml(item.value)}</div>
      </div>
    `,
    )
    .join("");
};

const renderHistorical = (historical) => {
  if (!historical || !historical.summary) {
    historicalCardsEl.innerHTML = "";
    historicalHighlightsEl.innerHTML = `<div class="list-item"><div class="meta">${escapeHtml(t("histNoData"))}</div></div>`;
    historicalProductsEl.innerHTML = "";
    return;
  }

  const summary = historical.summary;
  const ingestion = historical.ingestion || {};

  const cards = [
    { label: t("histCardObservations"), value: numberFmt.format(summary.observations || 0) },
    { label: t("histCardUniqueTrains"), value: numberFmt.format(summary.uniqueTrains || 0) },
    { label: t("histCardAvgDelay"), value: `${Number(summary.avgDelay || 0).toFixed(1)} ${t("minutes")}` },
    { label: t("histCardMaxDelay"), value: `${summary.maxDelay || 0} ${t("minutes")}` },
    { label: t("histCardAccumDelay"), value: `${numberFmt.format(Math.round(summary.accumulatedDelayMinutes || 0))} ${t("minutes")}` },
    { label: t("histCardOnTimePct"), value: `${Number(summary.onTimePct || 0).toFixed(1)}%` },
    { label: t("histCardSeverePct"), value: `${Number(summary.severePct || 0).toFixed(1)}%` },
    { label: t("histCardAvgBatch"), value: numberFmt.format(Math.round(ingestion.avgTrainsPerBatch || 0)) },
  ];

  historicalCardsEl.innerHTML = cards
    .map(
      (card) => `
      <article class="card">
        <div class="label">${escapeHtml(card.label)}</div>
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
        <div class="meta">${numberFmt.format(Math.round(topType.accumulatedDelayMinutes || 0))} ${t("minutes")} · ${numberFmt.format(topType.observations || 0)} ${t("histObsShort")} · ${numberFmt.format(topType.trains || 0)} ${t("histTrainsShort")}</div>
      </div>
    `);
  }

  if (topCorridor) {
    const corridorName = topCorridor.corridor || t("histNoCorridor");
    highlightRows.push(`
      <div class="list-item">
        <div class="name">${escapeHtml(t("histTopCorridor"))}: ${escapeHtml(corridorName)}</div>
        <div class="meta">${numberFmt.format(Math.round(topCorridor.accumulatedDelayMinutes || 0))} ${t("minutes")} · ${numberFmt.format(topCorridor.observations || 0)} ${t("histObsShort")} · ${numberFmt.format(topCorridor.trains || 0)} ${t("histTrainsShort")}</div>
      </div>
    `);
  }

  historicalHighlightsEl.innerHTML =
    highlightRows.length > 0
      ? highlightRows.join("")
      : `<div class="list-item"><div class="meta">${escapeHtml(t("histNoData"))}</div></div>`;

  const byProduct = Array.isArray(historical.byProduct) ? historical.byProduct : [];
  const maxObs = byProduct.reduce((max, item) => Math.max(max, Number(item.observations || 0)), 1);
  const rows = byProduct.slice(0, 8).map((item) => ({
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
  if (!Array.isArray(data.items) || data.items.length === 0) {
    trainsBodyEl.innerHTML = `<tr><td colspan="7">${escapeHtml(t("noRows"))}</td></tr>`;
    return;
  }

  trainsBodyEl.innerHTML = data.items
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

  renderOverviewCards(data.overview, data.today, data.typeInsights);
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

filtersForm.addEventListener("submit", (event) => {
  event.preventDefault();

  state.query = searchInput.value;
  state.minDelay = minDelayInput.value;
  state.offset = 0;

  void loadTrains();
});

langSwitchEl.addEventListener("change", () => {
  state.lang = langSwitchEl.value === "en" ? "en" : "es";
  localStorage.setItem("retrasometro_lang", state.lang);
  localStorage.removeItem("renfe_lang");
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
  const docsUrl = buildUrl("/api-docs.html");
  const opened = window.open(docsUrl, "_blank", "noopener,noreferrer");
  if (!opened) {
    setStatus(t("docsOpenFail"), "error");
  }
});

rawToggleBtnEl.addEventListener("click", () => {
  toggleRawPanel();
});

closeRawBtnEl.addEventListener("click", () => {
  if (state.isRawOpen) {
    toggleRawPanel();
  }
});

copyRawBtnEl.addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(buildUrl("/api/raw/live"));
    setStatus(t("rawCopyOk"), "ok");
  } catch {
    setStatus(t("rawCopyFail"), "error");
  }
});

recoverHistoryBtnEl.addEventListener("click", async () => {
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

const boot = async () => {
  langSwitchEl.value = state.lang;
  applyStaticTexts();
  setStatus(t("statusLoading"));

  try {
    await refresh();
  } catch (error) {
    setStatus(error instanceof Error ? error.message : String(error), "error");
  }

  setInterval(() => {
    void refresh();
  }, 20_000);
};

void boot();
