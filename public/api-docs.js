const docsLangSwitchEl = document.querySelector("#docs-lang-switch");
const docsStatusEl = document.querySelector("#docs-status");
const requestKeyBtnEl = document.querySelector("#request-key-btn");
const backHomeBtnEl = document.querySelector("#back-home-btn");
const authStepsEl = document.querySelector("#auth-steps");
const keyPanelEl = document.querySelector("#key-panel");
const keyValueEl = document.querySelector("#key-value");
const keyExpiryEl = document.querySelector("#key-expiry");
const copyKeyBtnEl = document.querySelector("#copy-key-btn");
const copyCurlBtnEl = document.querySelector("#copy-curl-btn");
const endpointsListEl = document.querySelector("#endpoints-list");
const curlExampleEl = document.querySelector("#curl-example");

const I18N = {
  es: {
    kicker: "retrasometro API",
    title: "Referencia API y acceso con clave temporal",
    subtitle:
      "Esta vista está separada del panel principal. Solicita aquí la clave temporal y consulta ejemplos reales.",
    langLabel: "Idioma",
    backHome: "Volver al panel",
    loading: "Cargando...",
    securityTitle: "Autenticación y límites",
    askKey: "Pedir clave temporal",
    keyLabel: "Clave API",
    keyExpiry: "Activa hasta",
    copyKey: "Copiar clave",
    copyCurl: "Copiar cURL",
    endpointsTitle: "Referencia de endpoints",
    exampleTitle: "Ejemplo de uso",
    publicTag: "Público",
    protectedTag: "Protegido",
    parameters: "Parámetros",
    headers: "Cabeceras",
    sampleRequest: "Solicitud ejemplo",
    sampleResponse: "Respuesta ejemplo (datos reales)",
    noParameters: "Sin parámetros.",
    noHeaders: "Sin cabeceras específicas.",
    required: "obligatorio",
    optional: "opcional",
    keyOk: "Clave solicitada correctamente",
    keyError: "No se pudo solicitar la clave",
    copyOk: "Copiado al portapapeles",
    copyError: "No se pudo copiar",
    docsError: "No se pudo cargar la documentación",
    methodLabel: "Método",
    pathLabel: "Ruta",
  },
  en: {
    kicker: "retrasometro API",
    title: "API reference and temporary key access",
    subtitle:
      "This view is separated from the main dashboard. Request your temporary key and inspect real response examples.",
    langLabel: "Language",
    backHome: "Back to dashboard",
    loading: "Loading...",
    securityTitle: "Authentication and limits",
    askKey: "Request temporary key",
    keyLabel: "API key",
    keyExpiry: "Active until",
    copyKey: "Copy key",
    copyCurl: "Copy cURL",
    endpointsTitle: "Endpoint reference",
    exampleTitle: "Usage example",
    publicTag: "Public",
    protectedTag: "Protected",
    parameters: "Parameters",
    headers: "Headers",
    sampleRequest: "Sample request",
    sampleResponse: "Sample response (real data)",
    noParameters: "No parameters.",
    noHeaders: "No specific headers.",
    required: "required",
    optional: "optional",
    keyOk: "Key requested successfully",
    keyError: "Could not request key",
    copyOk: "Copied to clipboard",
    copyError: "Could not copy",
    docsError: "Could not load docs",
    methodLabel: "Method",
    pathLabel: "Path",
  },
};

const state = {
  lang: (localStorage.getItem("retrasometro_lang") ?? localStorage.getItem("renfe_lang")) === "en" ? "en" : "es",
  docs: null,
  apiKey: null,
  apiKeyExpiresAt: null,
};

const t = (key) => I18N[state.lang][key] || key;
const locale = () => (state.lang === "es" ? "es-ES" : "en-US");

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

const setStatus = (message, type = "") => {
  docsStatusEl.textContent = message;
  docsStatusEl.classList.remove("ok", "error");
  if (type) {
    docsStatusEl.classList.add(type);
  }
};

const setText = (id, value) => {
  const element = document.querySelector(`#${id}`);
  if (element) {
    element.textContent = value;
  }
};

const buildUrl = (path) => {
  return new URL(path, window.location.origin).toString();
};

const endpointMethod = (endpoint) => {
  if (endpoint?.method) {
    return String(endpoint.method).toUpperCase();
  }

  const route = String(endpoint?.route || "");
  const [method] = route.split(" ");
  return method ? method.toUpperCase() : "GET";
};

const endpointPath = (endpoint) => {
  if (endpoint?.path) {
    return String(endpoint.path);
  }

  const route = String(endpoint?.route || "");
  const [, path = ""] = route.split(" ");
  return path || "/";
};

const withQuerySample = (endpoint) => {
  const path = endpointPath(endpoint);
  const query = Array.isArray(endpoint?.query) ? endpoint.query : [];

  if (query.length === 0) {
    return path;
  }

  const params = query
    .map((param) => {
      const value = param?.example;
      if (value === undefined || value === null || String(value).length === 0) {
        return `${param.name}=...`;
      }
      return `${param.name}=${encodeURIComponent(String(value))}`;
    })
    .join("&");

  return `${path}?${params}`;
};

const renderParamRows = (items, emptyMessage) => {
  if (!Array.isArray(items) || items.length === 0) {
    return `<div class="endpoint-empty">${escapeHtml(emptyMessage)}</div>`;
  }

  return items
    .map((item) => {
      const requirement = item.required ? t("required") : t("optional");
      const type = item.type ? `<span class="param-type">${escapeHtml(item.type)}</span>` : "";
      const description = item.description ? `<div class="param-desc">${escapeHtml(item.description)}</div>` : "";
      const example =
        item.example !== undefined && item.example !== null
          ? `<code class="param-example">${escapeHtml(String(item.example))}</code>`
          : "";

      return `
        <div class="param-row">
          <div class="param-main">
            <code class="param-name">${escapeHtml(item.name || "-")}</code>
            <span class="param-req">${escapeHtml(requirement)}</span>
            ${type}
          </div>
          ${description}
          ${example}
        </div>
      `;
    })
    .join("");
};

const buildCurlSnippet = (endpoint) => {
  const method = endpointMethod(endpoint);
  const target = withQuerySample(endpoint);
  const lines = [];

  lines.push("KEY='<API_KEY>'");

  const methodPart = method === "GET" ? "" : `-X ${method} `;
  const authHeader = endpoint.protected ? '-H "x-api-key: $KEY" ' : "";
  const langHeader = `-H "accept-language: ${state.lang}" `;

  lines.push(`curl -s ${methodPart}${authHeader}${langHeader}'${buildUrl(target)}' | jq .`);

  return lines.join("\n");
};

const renderAuthSteps = () => {
  const steps = state.docs?.details?.authFlow;
  if (!Array.isArray(steps) || steps.length === 0) {
    authStepsEl.innerHTML = "";
    return;
  }

  authStepsEl.innerHTML = steps
    .map(
      (step) => `
      <article class="step-card">
        <div class="step-id">${escapeHtml(String(step.step))}</div>
        <div class="step-body">
          <div class="step-title">${escapeHtml(step.title || "-")}</div>
          <div class="step-detail">${escapeHtml(step.detail || "")}</div>
        </div>
      </article>
    `,
    )
    .join("");
};

const renderEndpoints = () => {
  const endpoints = state.docs?.details?.endpoints;
  if (!Array.isArray(endpoints) || endpoints.length === 0) {
    endpointsListEl.innerHTML = "";
    return;
  }

  endpointsListEl.innerHTML = endpoints
    .map((endpoint) => {
      const method = endpointMethod(endpoint);
      const path = endpointPath(endpoint);
      const methodClass = method.toLowerCase();
      const scopeTag = endpoint.protected ? t("protectedTag") : t("publicTag");
      const scopeClass = endpoint.protected ? "protected" : "public";
      const paramsHtml = renderParamRows(endpoint.query, t("noParameters"));
      const headersHtml = renderParamRows(endpoint.headers, t("noHeaders"));
      const sampleResponse = endpoint.sampleResponse ?? {};

      return `
        <article class="endpoint-card">
          <div class="endpoint-head">
            <span class="method-tag method-${escapeHtml(methodClass)}">${escapeHtml(method)}</span>
            <code class="endpoint-path">${escapeHtml(path)}</code>
            <span class="scope-tag ${scopeClass}">${escapeHtml(scopeTag)}</span>
          </div>

          <p class="endpoint-desc">${escapeHtml(endpoint.description || "")}</p>

          <div class="endpoint-columns">
            <div class="endpoint-col">
              <div class="endpoint-subtitle">${escapeHtml(t("parameters"))}</div>
              <div class="param-list">${paramsHtml}</div>

              <div class="endpoint-subtitle">${escapeHtml(t("headers"))}</div>
              <div class="param-list">${headersHtml}</div>

              <div class="endpoint-subtitle">${escapeHtml(t("sampleRequest"))}</div>
              <pre class="endpoint-code">${escapeHtml(buildCurlSnippet(endpoint))}</pre>
            </div>

            <div class="endpoint-col">
              <div class="endpoint-subtitle">${escapeHtml(t("sampleResponse"))}</div>
              <pre class="endpoint-json">${escapeHtml(JSON.stringify(sampleResponse, null, 2))}</pre>
            </div>
          </div>
        </article>
      `;
    })
    .join("");
};

const updateCurlExample = () => {
  const key = state.apiKey || "<API_KEY>";
  curlExampleEl.textContent = [
    `KEY='${key}'`,
    `curl -s -H "x-api-key: $KEY" -H "accept-language: ${state.lang}" '${buildUrl("/api/dashboard?historyHours=168")}' | jq .`,
    `curl -s -H "x-api-key: $KEY" -H "accept-language: ${state.lang}" '${buildUrl("/api/raw/live")}' | jq .`,
  ].join("\n");
};

const renderKeyPanel = () => {
  if (!state.apiKey || !state.apiKeyExpiresAt) {
    keyPanelEl.classList.add("hidden");
    return;
  }

  keyPanelEl.classList.remove("hidden");
  keyValueEl.textContent = state.apiKey;
  const expiry = new Date(state.apiKeyExpiresAt).toLocaleString(locale(), { hour12: false });
  keyExpiryEl.textContent = `${t("keyExpiry")}: ${expiry}`;
  updateCurlExample();
};

const applyTexts = () => {
  document.documentElement.lang = state.lang;

  setText("docs-kicker", t("kicker"));
  setText("docs-title", t("title"));
  setText("docs-subtitle", t("subtitle"));
  setText("docs-lang-label", t("langLabel"));
  setText("back-home-btn", t("backHome"));
  setText("security-title", t("securityTitle"));
  setText("request-key-btn", t("askKey"));
  setText("key-label", t("keyLabel"));
  setText("copy-key-btn", t("copyKey"));
  setText("copy-curl-btn", t("copyCurl"));
  setText("endpoints-title", t("endpointsTitle"));
  setText("example-title", t("exampleTitle"));

  if (!state.docs) {
    setStatus(t("loading"));
  }

  const summary = state.docs?.details?.security?.message || "";
  setText("security-summary", summary);

  renderAuthSteps();
  renderEndpoints();
  renderKeyPanel();
  updateCurlExample();
};

const loadDocs = async () => {
  try {
    const response = await fetch(buildUrl("/api/docs"), {
      cache: "no-store",
      headers: {
        "accept-language": state.lang,
      },
    });

    if (!response.ok) {
      throw new Error(`${response.status}`);
    }

    state.docs = await response.json();
    setStatus(state.docs?.title || "API", "ok");
    applyTexts();
  } catch (error) {
    setStatus(`${t("docsError")}: ${error instanceof Error ? error.message : String(error)}`, "error");
  }
};

const requestKey = async () => {
  try {
    const response = await fetch(buildUrl("/api/auth/request-key"), {
      cache: "no-store",
      headers: {
        "accept-language": state.lang,
      },
    });

    if (!response.ok) {
      throw new Error(`${response.status}`);
    }

    const data = await response.json();
    state.apiKey = data.apiKey;
    state.apiKeyExpiresAt = Number(data.expiresAt || 0);
    renderKeyPanel();
    setStatus(t("keyOk"), "ok");
  } catch (error) {
    setStatus(`${t("keyError")}: ${error instanceof Error ? error.message : String(error)}`, "error");
  }
};

requestKeyBtnEl.addEventListener("click", () => {
  void requestKey();
});

copyKeyBtnEl.addEventListener("click", async () => {
  if (!state.apiKey) {
    return;
  }

  try {
    await navigator.clipboard.writeText(state.apiKey);
    setStatus(t("copyOk"), "ok");
  } catch {
    setStatus(t("copyError"), "error");
  }
});

copyCurlBtnEl.addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(curlExampleEl.textContent || "");
    setStatus(t("copyOk"), "ok");
  } catch {
    setStatus(t("copyError"), "error");
  }
});

backHomeBtnEl.addEventListener("click", () => {
  window.location.href = buildUrl("/");
});

docsLangSwitchEl.addEventListener("change", () => {
  state.lang = docsLangSwitchEl.value === "en" ? "en" : "es";
  localStorage.setItem("retrasometro_lang", state.lang);
  localStorage.removeItem("renfe_lang");
  applyTexts();
  void loadDocs();
});

const boot = async () => {
  docsLangSwitchEl.value = state.lang;
  applyTexts();
  await loadDocs();
};

void boot();
