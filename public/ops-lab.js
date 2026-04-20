import { initThemeController } from "./theme.js";

const langSwitchEl = document.querySelector("#ops-lang-switch");
const themeSwitchEl = document.querySelector("#ops-theme-switch");
const themeOptionSystemEl = document.querySelector("#ops-theme-option-system");
const themeOptionLightEl = document.querySelector("#ops-theme-option-light");
const themeOptionDarkEl = document.querySelector("#ops-theme-option-dark");
const backBtnEl = document.querySelector("#ops-back-btn");
const statusPillEl = document.querySelector("#ops-status-pill");
const windowButtons = [...document.querySelectorAll("[data-window]")];
const summaryCardsEl = document.querySelector("#ops-summary-cards");
const confidenceCardsEl = document.querySelector("#ops-confidence-cards");
const cacheMetaEl = document.querySelector("#ops-cache-meta");
const routesSearchEl = document.querySelector("#ops-routes-search");
const productsSearchEl = document.querySelector("#ops-products-search");
const routesBodyEl = document.querySelector("#ops-routes-body");
const productsBodyEl = document.querySelector("#ops-products-body");

const I18N = {
  es: {
    title: "Ops Lab",
    langLabel: "Idioma",
    themeLabel: "Tema",
    themeSystem: "Sistema",
    themeLight: "Claro",
    themeDark: "Oscuro",
    back: "Inicio",
    loading: "Cargando...",
    ready: "Datos operativos listos",
    error: "Error cargando métricas operativas",
    summaryTitle: "Resumen operativo",
    confidenceTitle: "Confianza del dato",
    routesTitle: "Ranking por dirección",
    productsTitle: "Ranking por tipo",
    routesSearch: "Filtrar por origen/destino",
    productsSearch: "Filtrar por tipo",
    colRoute: "Dirección",
    colRisk: "Riesgo P15",
    colSeverity: "Severidad p95",
    colLoad: "Carga TM60",
    colProduct: "Tipo",
    noData: "Sin datos",
    p15: "P15",
    p60: "P60",
    otp5: "OTP5",
    servicesObserved: "Servicios observados",
    kmObserved: "Km observados",
    tm15Hours: "TM15 (h)",
    tm60Hours: "TM60 (h)",
    freshness: "Frescura (s)",
    pctLive: "% live",
    pctRecovered: "% recovered",
    pctRepeated: "% repeated",
    pctOfficialCorridor: "% corredor oficial",
    pctOriginMatch: "% origen válido",
    pctDestinationMatch: "% destino válido",
    pctSynthetic: "% id sintético",
    cacheSource: "Fuente",
    cacheAge: "edad",
    cacheWindow: "ventana",
    rowSuffixServices: "servicios",
    rowSuffixP60: "P60",
    rowSuffixTm: "TM60/100",
  },
  en: {
    title: "Ops Lab",
    langLabel: "Language",
    themeLabel: "Theme",
    themeSystem: "System",
    themeLight: "Light",
    themeDark: "Dark",
    back: "Home",
    loading: "Loading...",
    ready: "Operational data ready",
    error: "Error loading operational metrics",
    summaryTitle: "Operational summary",
    confidenceTitle: "Data confidence",
    routesTitle: "Direction ranking",
    productsTitle: "Product ranking",
    routesSearch: "Filter by origin/destination",
    productsSearch: "Filter by product",
    colRoute: "Direction",
    colRisk: "Risk P15",
    colSeverity: "Severity p95",
    colLoad: "Load TM60",
    colProduct: "Product",
    noData: "No data",
    p15: "P15",
    p60: "P60",
    otp5: "OTP5",
    servicesObserved: "Observed services",
    kmObserved: "Observed km",
    tm15Hours: "TM15 (h)",
    tm60Hours: "TM60 (h)",
    freshness: "Freshness (s)",
    pctLive: "% live",
    pctRecovered: "% recovered",
    pctRepeated: "% repeated",
    pctOfficialCorridor: "% official corridor",
    pctOriginMatch: "% valid origin",
    pctDestinationMatch: "% valid destination",
    pctSynthetic: "% synthetic id",
    cacheSource: "Source",
    cacheAge: "age",
    cacheWindow: "window",
    rowSuffixServices: "services",
    rowSuffixP60: "P60",
    rowSuffixTm: "TM60/100",
  },
};

const state = {
  lang: (localStorage.getItem("retrasometro_lang") ?? localStorage.getItem("renfe_lang")) === "en" ? "en" : "es",
  key: null,
  windowHours: 168,
  payload: null,
};

const t = (key) => I18N[state.lang][key] || key;

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

const setStatus = (message, type = "loading") => {
  statusPillEl.textContent = message;
  statusPillEl.classList.remove("error");
  if (type === "error") {
    statusPillEl.classList.add("error");
  }
};

const formatNumber = (value, digits = 1) => {
  if (!Number.isFinite(value)) {
    return "0";
  }

  return new Intl.NumberFormat(state.lang === "es" ? "es-ES" : "en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: 0,
  }).format(value);
};

const setTranslations = () => {
  document.querySelector("#ops-title").textContent = t("title");
  document.querySelector("#ops-lang-label").textContent = t("langLabel");
  document.querySelector("#ops-theme-label").textContent = t("themeLabel");
  themeOptionSystemEl.textContent = t("themeSystem");
  themeOptionLightEl.textContent = t("themeLight");
  themeOptionDarkEl.textContent = t("themeDark");
  backBtnEl.textContent = t("back");
  document.querySelector("#ops-summary-title").textContent = t("summaryTitle");
  document.querySelector("#ops-confidence-title").textContent = t("confidenceTitle");
  document.querySelector("#ops-routes-title").textContent = t("routesTitle");
  document.querySelector("#ops-products-title").textContent = t("productsTitle");
  routesSearchEl.placeholder = t("routesSearch");
  productsSearchEl.placeholder = t("productsSearch");
  document.querySelector("#ops-col-route").textContent = t("colRoute");
  document.querySelector("#ops-col-risk").textContent = t("colRisk");
  document.querySelector("#ops-col-severity").textContent = t("colSeverity");
  document.querySelector("#ops-col-load").textContent = t("colLoad");
  document.querySelector("#ops-col-product").textContent = t("colProduct");
  document.querySelector("#ops-col-product-risk").textContent = t("colRisk");
  document.querySelector("#ops-col-product-severity").textContent = t("colSeverity");
  document.querySelector("#ops-col-product-load").textContent = t("colLoad");
};

const renderCards = (target, cards) => {
  target.innerHTML = cards
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

const toUnifiedRows = (group) => {
  const unified = new Map();
  const upsert = (items, bucket) => {
    for (const item of items) {
      const existing = unified.get(item.key) ?? {
        key: item.key,
        label: item.productName || item.label,
        services: item.services,
        risk: null,
        severity: null,
        load: null,
      };
      existing.label = item.productName || item.label;
      existing.services = Math.max(existing.services, item.services);
      existing[bucket] = item;
      unified.set(item.key, existing);
    }
  };

  upsert(group.risk || [], "risk");
  upsert(group.severity || [], "severity");
  upsert(group.load || [], "load");

  return [...unified.values()].sort((a, b) => {
    const aRisk = a.risk?.p15 ?? -1;
    const bRisk = b.risk?.p15 ?? -1;
    if (bRisk !== aRisk) {
      return bRisk - aRisk;
    }
    return (b.services || 0) - (a.services || 0);
  });
};

const renderRankingRows = (rows, tbody, filterText) => {
  const normalizedFilter = filterText.trim().toLowerCase();
  const filtered = rows.filter((row) => {
    if (!normalizedFilter) {
      return true;
    }

    const haystack = `${row.label} ${row.key}`.toLowerCase();
    return haystack.includes(normalizedFilter);
  });

  if (filtered.length === 0) {
    tbody.innerHTML = `<tr><td colspan="4">${escapeHtml(t("noData"))}</td></tr>`;
    return;
  }

  tbody.innerHTML = filtered
    .map((row) => {
      const risk = row.risk
        ? `${formatNumber(row.risk.p15)}% · ${formatNumber(row.risk.p60)}% ${t("rowSuffixP60")}`
        : "-";
      const severity = row.severity
        ? `${formatNumber(row.severity.p95PeakDelay, 1)} min`
        : "-";
      const load = row.load
        ? `${formatNumber(row.load.tm60Per100ServicesSec, 1)}s ${t("rowSuffixTm")}`
        : "-";
      const label = `${row.label} · ${formatNumber(row.services, 0)} ${t("rowSuffixServices")}`;
      return `
        <tr>
          <td>${escapeHtml(label)}</td>
          <td>${escapeHtml(risk)}</td>
          <td>${escapeHtml(severity)}</td>
          <td>${escapeHtml(load)}</td>
        </tr>
      `;
    })
    .join("");
};

const render = () => {
  const payload = state.payload;
  if (!payload) {
    return;
  }

  const summary = payload.summary || {};
  const confidence = payload.confidence || {};
  const tm = summary.tm || {};
  renderCards(summaryCardsEl, [
    { label: t("p15"), value: `${formatNumber(summary.p15)}%` },
    { label: t("p60"), value: `${formatNumber(summary.p60)}%` },
    { label: t("otp5"), value: `${formatNumber(summary.otp5)}%` },
    { label: t("servicesObserved"), value: formatNumber(summary.servicesObserved, 0) },
    { label: t("kmObserved"), value: formatNumber(summary.kmObserved, 1) },
    { label: t("tm15Hours"), value: formatNumber(tm.gt15Hours, 2) },
    { label: t("tm60Hours"), value: formatNumber(tm.gt60Hours, 2) },
  ]);

  renderCards(confidenceCardsEl, [
    {
      label: t("freshness"),
      value: confidence.freshnessSec === null ? "-" : formatNumber(confidence.freshnessSec, 0),
    },
    { label: t("pctLive"), value: `${formatNumber(confidence.pctLive)}%` },
    { label: t("pctRecovered"), value: `${formatNumber(confidence.pctRecovered)}%` },
    { label: t("pctRepeated"), value: `${formatNumber(confidence.pctRepeated)}%` },
    { label: t("pctOfficialCorridor"), value: `${formatNumber(confidence.pctOfficialCorridor)}%` },
    { label: t("pctOriginMatch"), value: `${formatNumber(confidence.pctOriginStationMatch)}%` },
    {
      label: t("pctDestinationMatch"),
      value: `${formatNumber(confidence.pctDestinationStationMatch)}%`,
    },
    { label: t("pctSynthetic"), value: `${formatNumber(confidence.pctSyntheticId)}%` },
  ]);

  const routes = toUnifiedRows(payload.rankings?.corridorDirection || {});
  const products = toUnifiedRows(payload.rankings?.product || {});
  renderRankingRows(routes, routesBodyEl, routesSearchEl.value || "");
  renderRankingRows(products, productsBodyEl, productsSearchEl.value || "");

  for (const button of windowButtons) {
    const active = Number(button.dataset.window) === state.windowHours;
    button.classList.toggle("active", active);
  }

  const cacheMeta = payload.cacheMeta || {};
  cacheMetaEl.textContent = `${t("cacheSource")}: ${cacheMeta.source} · ${t("cacheWindow")}: ${cacheMeta.windowHours}h · ${t("cacheAge")}: ${formatNumber(cacheMeta.ageSeconds, 0)}s`;
};

const requestApiKey = async () => {
  const response = await fetch("/api/auth/request-key");
  if (!response.ok) {
    throw new Error(`auth_${response.status}`);
  }

  const data = await response.json();
  if (!data?.apiKey) {
    throw new Error("auth_empty");
  }

  state.key = data.apiKey;
};

const loadOps = async () => {
  if (!state.key) {
    await requestApiKey();
  }

  const response = await fetch(`/api/ops?window=${state.windowHours}`, {
    headers: {
      "x-api-key": state.key,
      "accept-language": state.lang,
    },
  });

  if (response.status === 401 || response.status === 429) {
    await requestApiKey();
    return loadOps();
  }

  if (!response.ok) {
    throw new Error(`ops_${response.status}`);
  }

  const data = await response.json();
  state.payload = data;
};

const refresh = async () => {
  setStatus(t("loading"));
  try {
    await loadOps();
    render();
    setStatus(t("ready"), "ok");
  } catch (error) {
    console.error("[ops-lab]", error);
    setStatus(t("error"), "error");
  }
};

const init = async () => {
  langSwitchEl.value = state.lang;
  initThemeController({
    selectEl: themeSwitchEl,
  });

  langSwitchEl.addEventListener("change", () => {
    state.lang = langSwitchEl.value === "en" ? "en" : "es";
    localStorage.setItem("retrasometro_lang", state.lang);
    setTranslations();
    render();
    void refresh();
  });

  backBtnEl.addEventListener("click", () => {
    window.location.href = "/";
  });

  for (const button of windowButtons) {
    button.addEventListener("click", () => {
      const nextWindow = Number(button.dataset.window);
      if (![24, 168, 720].includes(nextWindow)) {
        return;
      }

      state.windowHours = nextWindow;
      void refresh();
    });
  }

  routesSearchEl.addEventListener("input", () => {
    render();
  });
  productsSearchEl.addEventListener("input", () => {
    render();
  });

  setTranslations();
  await refresh();
};

void init();
