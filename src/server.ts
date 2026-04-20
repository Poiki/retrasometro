import { join, normalize, extname } from "node:path";
import { brotliCompressSync, constants as zlibConstants, gzipSync } from "node:zlib";
import { config } from "./config";
import { DB } from "./db";
import { RenfeIngestor } from "./ingestor";
import { DashboardPresetCache } from "./dashboard-cache";
import { OpsPresetCache } from "./ops-cache";
import { ApiKeyManager } from "./auth";
import { getProductName, getProductNames, type AppLanguage } from "./products";
import { resolveLanguage, t } from "./i18n";

const publicRoot = join(import.meta.dir, "..", "public");

const json = (body: unknown, init?: ResponseInit) => {
  return Response.json(body, {
    headers: {
      "cache-control": "no-store",
    },
    ...init,
  });
};

const isCompressibleType = (contentType: string | null): boolean => {
  if (!contentType) {
    return false;
  }

  const type = contentType.toLowerCase();
  return (
    type.includes("application/json") ||
    type.includes("text/") ||
    type.includes("application/javascript") ||
    type.includes("text/css")
  );
};

const pickCompressionEncoding = (acceptEncoding: string | null): "br" | "gzip" | null => {
  if (!acceptEncoding) {
    return null;
  }

  const lower = acceptEncoding.toLowerCase();
  if (lower.includes("br")) {
    return "br";
  }

  if (lower.includes("gzip")) {
    return "gzip";
  }

  return null;
};

const maybeCompressResponse = async (request: Request, response: Response): Promise<Response> => {
  if (!config.httpCompressionEnabled || request.method === "HEAD") {
    return response;
  }

  if (response.status < 200 || response.status >= 300 || response.body === null) {
    return response;
  }

  if (response.headers.has("content-encoding")) {
    return response;
  }

  if (!isCompressibleType(response.headers.get("content-type"))) {
    return response;
  }

  const encoding = pickCompressionEncoding(request.headers.get("accept-encoding"));
  if (!encoding) {
    return response;
  }

  const bodyBuffer = Buffer.from(await response.arrayBuffer());
  if (bodyBuffer.byteLength < config.httpCompressionMinBytes) {
    return new Response(bodyBuffer, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
  }

  const compressedBody =
    encoding === "br"
      ? brotliCompressSync(bodyBuffer, {
          params: {
            [zlibConstants.BROTLI_PARAM_QUALITY]: Math.max(1, Math.min(11, config.httpBrotliLevel)),
          },
        })
      : gzipSync(bodyBuffer, {
          level: Math.max(1, Math.min(9, config.httpGzipLevel)),
        });

  if (compressedBody.byteLength >= bodyBuffer.byteLength) {
    return new Response(bodyBuffer, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
  }

  const headers = new Headers(response.headers);
  const previousVary = headers.get("vary");
  const nextVary = previousVary
    ? previousVary
        .split(",")
        .map((entry) => entry.trim().toLowerCase())
        .includes("accept-encoding")
      ? previousVary
      : `${previousVary}, Accept-Encoding`
    : "Accept-Encoding";
  headers.set("content-encoding", encoding);
  headers.set("content-length", String(compressedBody.byteLength));
  headers.set("vary", nextVary);

  return new Response(compressedBody, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
};

const toIso = (value: number | null) => {
  if (!value) {
    return null;
  }

  return new Date(value * 1000).toISOString();
};

const pickTrainDocSample = (train: Record<string, any>) => ({
  codComercial: train.cod_comercial ?? train.codComercial ?? null,
  codProduct: train.cod_product ?? train.codProduct ?? null,
  productName: train.product_name ?? train.productName ?? null,
  corridor: train.des_corridor ?? train.desCorridor ?? null,
  codOrigen: train.cod_origen ?? train.codOrigen ?? null,
  codDestino: train.cod_destino ?? train.codDestino ?? null,
  codEstSig: train.cod_est_sig ?? train.codEstSig ?? null,
  ultRetraso: train.ult_retraso ?? train.ultRetraso ?? null,
  latitud: train.latitud ?? null,
  longitud: train.longitud ?? null,
  lastSeenAt: train.last_seen_at ?? train.lastSeenAt ?? null,
  lastSeenAtIso: toIso(train.last_seen_at ?? train.lastSeenAt ?? null),
});

const parseIntOrNull = (value: string | null): number | null => {
  if (!value) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : null;
};

const parseEpochOrDate = (value: string | null): number | null => {
  if (!value) {
    return null;
  }

  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return null;
  }

  if (/^\d+$/.test(trimmed)) {
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? Math.trunc(parsed) : null;
  }

  const ms = Date.parse(trimmed);
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : null;
};

const withProductLabels = <T extends { cod_product: number }>(rows: T[], lang: AppLanguage) => {
  return rows.map((row) => {
    const names = getProductNames(row.cod_product);
    return {
      ...row,
      product_name: getProductName(row.cod_product, lang),
      product_name_es: names.es,
      product_name_en: names.en,
    };
  });
};

const i18nMessage = (key: Parameters<typeof t>[1]) => ({
  es: t("es", key),
  en: t("en", key),
});

const asError = (lang: AppLanguage, key: Parameters<typeof t>[1], code: string, status: number) => {
  return json(
    {
      ok: false,
      code,
      language: lang,
      message: t(lang, key),
      messages: i18nMessage(key),
    },
    { status },
  );
};

const isProtectedPath = (pathname: string): boolean => {
  if (pathname === "/api/dashboard") {
    return true;
  }

  if (pathname === "/api/ops") {
    return true;
  }

  if (pathname === "/api/trains") {
    return true;
  }

  if (pathname.startsWith("/api/trains/") && pathname.endsWith("/history")) {
    return true;
  }

  if (pathname === "/api/ingestion/runs") {
    return true;
  }

  if (pathname === "/api/raw/live") {
    return true;
  }

  if (pathname === "/api/history/coverage") {
    return true;
  }

  if (pathname === "/api/history/recover") {
    return true;
  }

  return false;
};

export const startServer = (
  db: DB,
  ingestor: RenfeIngestor,
  dashboardPresetCache: DashboardPresetCache,
  opsPresetCache: OpsPresetCache,
) => {
  const apiKeyManager = new ApiKeyManager();

  const server = Bun.serve({
    port: config.port,
    async fetch(request) {
      const url = new URL(request.url);
      const response = url.pathname.startsWith("/api/")
        ? await handleApi(
            request,
            url,
            db,
            ingestor,
            dashboardPresetCache,
            opsPresetCache,
            apiKeyManager,
          )
        : await serveStatic(url.pathname);

      return maybeCompressResponse(request, response);
    },
  });

  console.log(`[server] http://localhost:${server.port}`);
  return server;
};

const handleApi = async (
  request: Request,
  url: URL,
  db: DB,
  ingestor: RenfeIngestor,
  dashboardPresetCache: DashboardPresetCache,
  opsPresetCache: OpsPresetCache,
  apiKeyManager: ApiKeyManager,
): Promise<Response> => {
  const lang = resolveLanguage(url, request);

  if (url.pathname === "/api/auth/request-key") {
    const keyData = apiKeyManager.issueKey();

    return json({
      ok: true,
      language: lang,
      message: t(lang, "keyIssued"),
      messages: i18nMessage("keyIssued"),
      apiKey: keyData.key,
      createdAt: keyData.createdAt,
      createdAtIso: new Date(keyData.createdAt).toISOString(),
      expiresAt: keyData.expiresAt,
      expiresAtIso: new Date(keyData.expiresAt).toISOString(),
      expiresInSeconds: keyData.expiresInSeconds,
      limits: keyData.limits,
      docsUrl: "/api/docs",
    });
  }

  if (url.pathname === "/api/docs") {
    const sampleOverview = db.getOverview();
    const sampleDelayBuckets = db.getDelayBuckets();
    const sampleToday = db.getTodayAggregate();
    const sampleHistory = db.getHistoricalStats(24);
    const sampleTopCorridors = db.getTopCorridors(3);
    const sampleCorridorMeta = db.getCorridorMeta();
    const sampleCoverage = db.getHistoryCoverage(48, Math.floor(config.pollingIntervalMs / 1000));
    const sampleStorage = db.getObservationArchiveStorage();
    const sampleOps = db.getOpsMetrics(24);
    const sampleTrains = withProductLabels(
      db.listTrains({
        query: null,
        minDelay: null,
        limit: 2,
        offset: 0,
      }) as Array<{ cod_product: number }>,
      lang,
    ).map((item) => pickTrainDocSample(item as Record<string, any>));
    const sampleProducts = db
      .getProductMetrics()
      .slice(0, 3)
      .map((item) => ({
        ...item,
        productName: getProductName(item.codProduct, lang),
        productNameEs: getProductName(item.codProduct, "es"),
        productNameEn: getProductName(item.codProduct, "en"),
      }));

    const responseSamples = {
      requestKey: {
        ok: true,
        language: lang,
        message: t(lang, "keyIssued"),
        apiKey: "temp_xxxxxxxxxxxxxxxxxxxxxxxx",
        createdAt: Date.now(),
        createdAtIso: new Date().toISOString(),
        expiresAt: Date.now() + config.apiKeyTtlSeconds * 1000,
        expiresAtIso: new Date(Date.now() + config.apiKeyTtlSeconds * 1000).toISOString(),
        expiresInSeconds: config.apiKeyTtlSeconds,
        limits: {
          maxConcurrentRequestsPerKey: 1,
          minIntervalMs: config.apiRateLimitMs,
        },
      },
      dashboard: {
        ok: true,
        language: lang,
        generatedAt: new Date().toISOString(),
        cacheMeta: {
          source: "cache",
          windowHours: 24,
          generatedAt: new Date().toISOString(),
          ageSeconds: 12,
          customRange: false,
        },
        historyPrecision: {
          mode: "raw_detail",
          rawWindowDays: config.observationRetentionDays,
          archivesAvailable: config.archiveEnabled,
        },
        overview: {
          ...sampleOverview,
          lastSeenAtIso: toIso(sampleOverview.lastSeenAt),
        },
        delayBuckets: sampleDelayBuckets,
        byProduct: sampleProducts,
        topCorridors: sampleTopCorridors,
        corridorMeta: sampleCorridorMeta,
        today: sampleToday,
        historical: {
          hours: 24,
          summary: sampleHistory.summary,
          topProblematicProduct: sampleHistory.topProblematicProduct
            ? {
                ...sampleHistory.topProblematicProduct,
                productName: getProductName(sampleHistory.topProblematicProduct.codProduct, lang),
                productNameEs: getProductName(sampleHistory.topProblematicProduct.codProduct, "es"),
                productNameEn: getProductName(sampleHistory.topProblematicProduct.codProduct, "en"),
              }
            : null,
          topProblematicCorridor: sampleHistory.topProblematicCorridor,
          accountability: sampleHistory.accountability,
        },
      },
      trains: {
        ok: true,
        language: lang,
        total: sampleOverview.activeTrains,
        limit: 2,
        offset: 0,
        items: sampleTrains,
      },
      ops: {
        ok: true,
        language: lang,
        generatedAt: new Date().toISOString(),
        cacheMeta: {
          source: "cache",
          windowHours: 24,
          generatedAt: sampleOps.generatedAt,
          ageSeconds: 10,
        },
        summary: sampleOps.summary,
        rankings: sampleOps.rankings,
        confidence: sampleOps.confidence,
      },
      rawLive: {
        ok: true,
        language: lang,
        generatedAt: new Date().toISOString(),
        source: {
          endpointList: config.endpointList,
          cacheFile: config.cacheFile,
          cacheAvailable: true,
        },
        overview: sampleOverview,
        delayBuckets: sampleDelayBuckets,
        today: sampleToday,
        historyCoverage: sampleCoverage,
        storage: sampleStorage,
        trainsCurrent: sampleTrains,
      },
      historyCoverage: {
        ok: true,
        language: lang,
        report: sampleCoverage,
        storage: sampleStorage,
      },
      errorRateLimit: {
        ok: false,
        code: "RATE_LIMITED",
        language: lang,
        message: t(lang, "rateLimited"),
      },
    };

    return json({
      ok: true,
      language: lang,
      title: t(lang, "docsTitle"),
      details: {
        security: {
          message: t(lang, "docsSecurity"),
          messageEs: t("es", "docsSecurity"),
          messageEn: t("en", "docsSecurity"),
          requestKey: {
            route: "GET /api/auth/request-key",
            description: t(lang, "docsRequestKey"),
          },
          limits: {
            maxConcurrentRequestsPerKey: 1,
            minIntervalMs: config.apiRateLimitMs,
            keyTtlSeconds: config.apiKeyTtlSeconds,
          },
        },
        authFlow: [
          {
            step: 1,
            title: lang === "es" ? "Solicita clave temporal" : "Request temporary key",
            detail:
              lang === "es"
                ? "Llama a GET /api/auth/request-key y guarda la clave devuelta."
                : "Call GET /api/auth/request-key and store the returned key.",
          },
          {
            step: 2,
            title: lang === "es" ? "Envía la cabecera x-api-key" : "Send x-api-key header",
            detail:
              lang === "es"
                ? "Incluye la clave en todas las rutas protegidas."
                : "Include the key in all protected routes.",
          },
          {
            step: 3,
            title: lang === "es" ? "Respeta los límites" : "Respect limits",
            detail:
              lang === "es"
                ? "Máximo 1 petición a la vez y 1 por segundo por clave."
                : "Maximum 1 in-flight request and 1 request/sec per key.",
          },
        ],
        endpoints: [
          {
            id: "dashboard",
            method: "GET",
            path: "/api/dashboard",
            route: "GET /api/dashboard?historyHours=24|168|720&historyFrom=ISO|epoch&historyTo=ISO|epoch",
            protected: true,
            description:
              lang === "es"
                ? "Métricas principales + estadísticas históricas (preset o rango personalizado)."
                : "Main dashboard metrics + historical stats (preset or custom range).",
            query: [
              {
                name: "historyHours",
                type: "integer",
                required: false,
                example: 168,
                description:
                  lang === "es" ? "Ventana en horas (24 a 720)." : "Window in hours (24 to 720).",
              },
              {
                name: "historyFrom",
                type: "ISO8601|epoch",
                required: false,
                example: "2026-03-01T00:00:00Z",
                description:
                  lang === "es" ? "Inicio del rango personalizado." : "Custom range start.",
              },
              {
                name: "historyTo",
                type: "ISO8601|epoch",
                required: false,
                example: "2026-03-10T23:59:59Z",
                description:
                  lang === "es" ? "Fin del rango personalizado." : "Custom range end.",
              },
            ],
            headers: [{ name: "x-api-key", required: true, description: "Temporary API key" }],
            sampleResponse: responseSamples.dashboard,
          },
          {
            id: "trains",
            method: "GET",
            path: "/api/trains",
            route: "GET /api/trains?q=&minDelay=&limit=&offset=",
            protected: true,
            description:
              lang === "es"
                ? "Listado de trenes con filtros."
                : "Train list with filters.",
            query: [
              {
                name: "q",
                type: "string",
                required: false,
                example: "Madrid",
                description:
                  lang === "es" ? "Busca por tren, corredor o estación." : "Search by train, corridor, or station.",
              },
              {
                name: "minDelay",
                type: "integer",
                required: false,
                example: 15,
                description:
                  lang === "es" ? "Filtra por retraso mínimo en minutos." : "Filter by minimum delay in minutes.",
              },
              {
                name: "limit",
                type: "integer",
                required: false,
                example: 50,
                description:
                  lang === "es" ? "Máximo de elementos por página." : "Maximum items per page.",
              },
              {
                name: "offset",
                type: "integer",
                required: false,
                example: 0,
                description:
                  lang === "es" ? "Desplazamiento para paginación." : "Pagination offset.",
              },
            ],
            headers: [{ name: "x-api-key", required: true, description: "Temporary API key" }],
            sampleResponse: responseSamples.trains,
          },
          {
            id: "ops",
            method: "GET",
            path: "/api/ops",
            route: "GET /api/ops?window=24|168|720",
            protected: true,
            description:
              lang === "es"
                ? "Métricas operativas V1 (incidencia, severidad, exposición y confianza)."
                : "Operational V1 metrics (incidence, severity, exposure and confidence).",
            query: [
              {
                name: "window",
                type: "integer",
                required: false,
                example: 168,
                description:
                  lang === "es"
                    ? "Ventana operativa: 24h, 7d o 30d."
                    : "Operational window: 24h, 7d or 30d.",
              },
            ],
            headers: [{ name: "x-api-key", required: true, description: "Temporary API key" }],
            sampleResponse: responseSamples.ops,
          },
          {
            id: "trainHistory",
            method: "GET",
            path: "/api/trains/:codComercial/history",
            route: "GET /api/trains/:codComercial/history?hours=24",
            protected: true,
            description:
              lang === "es"
                ? "Histórico individual por tren."
                : "Per-train history.",
            query: [
              {
                name: "hours",
                type: "integer",
                required: false,
                example: 24,
                description:
                  lang === "es" ? "Rango histórico para ese tren (1-168)." : "History range for this train (1-168).",
              },
            ],
            headers: [{ name: "x-api-key", required: true, description: "Temporary API key" }],
            sampleResponse: {
              ok: true,
              language: lang,
              codComercial: sampleTrains[0]?.codComercial ?? "00000",
              hours: 24,
              snapshots: [
                {
                  cod_comercial: sampleTrains[0]?.codComercial ?? "00000",
                  ts: Math.floor(Date.now() / 1000),
                  latitud: sampleTrains[0]?.latitud ?? 40.4,
                  longitud: sampleTrains[0]?.longitud ?? -3.7,
                  ult_retraso: sampleTrains[0]?.ultRetraso ?? 5,
                },
              ],
              observations: [
                {
                  cod_comercial: sampleTrains[0]?.codComercial ?? "00000",
                  captured_at: Math.floor(Date.now() / 1000),
                  ult_retraso: sampleTrains[0]?.ultRetraso ?? 5,
                },
              ],
              daily: [],
            },
          },
          {
            id: "ingestionRuns",
            method: "GET",
            path: "/api/ingestion/runs",
            route: "GET /api/ingestion/runs?limit=50",
            protected: true,
            description:
              lang === "es"
                ? "Auditoría de ejecuciones de ingesta."
                : "Ingestion run audit.",
            query: [
              {
                name: "limit",
                type: "integer",
                required: false,
                example: 50,
                description:
                  lang === "es" ? "Cantidad de ejecuciones a devolver." : "Number of runs to return.",
              },
            ],
            headers: [{ name: "x-api-key", required: true, description: "Temporary API key" }],
            sampleResponse: {
              ok: true,
              language: lang,
              items: db.getRecentRuns(2),
            },
          },
          {
            id: "rawLive",
            method: "GET",
            path: "/api/raw/live",
            route: "GET /api/raw/live",
            protected: true,
            description: t(lang, "docsRawEndpoint"),
            headers: [{ name: "x-api-key", required: true, description: "Temporary API key" }],
            sampleResponse: responseSamples.rawLive,
          },
          {
            id: "historyCoverage",
            method: "GET",
            path: "/api/history/coverage",
            route: "GET /api/history/coverage?hours=48",
            protected: true,
            description:
              lang === "es"
                ? "Cobertura histórica real y huecos detectados."
                : "Real historical coverage and detected gaps.",
            query: [
              {
                name: "hours",
                type: "integer",
                required: false,
                example: 48,
                description:
                  lang === "es" ? "Ventana a auditar (1-720)." : "Window to audit (1-720).",
              },
            ],
            headers: [{ name: "x-api-key", required: true, description: "Temporary API key" }],
            sampleResponse: responseSamples.historyCoverage,
          },
          {
            id: "historyRecover",
            method: "POST",
            path: "/api/history/recover",
            route: "POST /api/history/recover?hours=48",
            protected: true,
            description:
              lang === "es"
                ? "Recupera observaciones desde snapshots existentes."
                : "Recover observations from existing snapshots.",
            query: [
              {
                name: "hours",
                type: "integer",
                required: false,
                example: 48,
                description:
                  lang === "es"
                    ? "Ventana hacia atrás para recuperar observaciones."
                    : "Lookback window for recovering observations.",
              },
            ],
            headers: [{ name: "x-api-key", required: true, description: "Temporary API key" }],
            sampleResponse: {
              ok: true,
              language: lang,
              recovered: 12,
              report: sampleCoverage,
            },
          },
          {
            id: "health",
            method: "GET",
            path: "/api/health",
            route: "GET /api/health",
            protected: false,
            description:
              lang === "es"
                ? "Estado del servicio y de la ingesta."
                : "Service and ingestion health.",
            sampleResponse: {
              ok: true,
              language: lang,
              message: t(lang, "healthOk"),
              now: new Date().toISOString(),
            },
          },
          {
            id: "requestKey",
            method: "GET",
            path: "/api/auth/request-key",
            route: "GET /api/auth/request-key",
            protected: false,
            description:
              lang === "es"
                ? "Emite una clave temporal para consumir rutas protegidas."
                : "Issues a temporary key for protected routes.",
            sampleResponse: responseSamples.requestKey,
          },
        ],
        examples: responseSamples,
      },
    });
  }

  if (url.pathname === "/api/health") {
    return json({
      ok: true,
      language: lang,
      message: t(lang, "healthOk"),
      messages: i18nMessage("healthOk"),
      now: new Date().toISOString(),
      ingestor: ingestor.getStatus(),
      auth: apiKeyManager.getStats(),
    });
  }

  if (isProtectedPath(url.pathname)) {
    const apiKey = apiKeyManager.getKeyFromRequest(request);
    const lock = apiKeyManager.tryAcquire(apiKey);

    if (!lock.ok) {
      if (lock.reason === "missing") {
        return asError(lang, "missingApiKey", "MISSING_API_KEY", 401);
      }

      if (lock.reason === "invalid") {
        return asError(lang, "invalidApiKey", "INVALID_API_KEY", 401);
      }

      if (lock.reason === "expired") {
        return asError(lang, "expiredApiKey", "EXPIRED_API_KEY", 401);
      }

      if (lock.reason === "in_flight") {
        return asError(lang, "requestInProgress", "IN_FLIGHT_REQUEST", 429);
      }

      return asError(lang, "rateLimited", "RATE_LIMITED", 429);
    }

    try {
      return await handleProtectedApi(
        request,
        url,
        db,
        ingestor,
        dashboardPresetCache,
        opsPresetCache,
        lang,
      );
    } finally {
      lock.release();
    }
  }

  return asError(lang, "notFound", "NOT_FOUND", 404);
};

const handleProtectedApi = async (
  request: Request,
  url: URL,
  db: DB,
  ingestor: RenfeIngestor,
  dashboardPresetCache: DashboardPresetCache,
  opsPresetCache: OpsPresetCache,
  lang: AppLanguage,
): Promise<Response> => {
  if (url.pathname === "/api/dashboard") {
    const historyHours = Math.min(
      Math.max(parseIntOrNull(url.searchParams.get("historyHours")) ?? 168, 24),
      24 * 30,
    );
    const nowEpoch = Math.floor(Date.now() / 1000);
    const rawFrom = parseEpochOrDate(url.searchParams.get("historyFrom"));
    const rawTo = parseEpochOrDate(url.searchParams.get("historyTo"));
    const hasCustomRange = rawFrom !== null || rawTo !== null;
    const rawWindowDays = Math.max(0, Math.trunc(config.observationRetentionDays));
    const rawWindowSeconds = rawWindowDays > 0 ? rawWindowDays * 24 * 3600 : 0;
    const rawWindowStart = rawWindowSeconds > 0 ? nowEpoch - rawWindowSeconds : null;
    let historyPrecisionMode: "raw_detail" | "aggregated_only" | "mixed" = "raw_detail";

    const overview = db.getOverview();
    const delayBuckets = db.getDelayBuckets();
    const byProduct = db.getProductMetrics().map((item) => {
      const names = getProductNames(item.codProduct);

      return {
        ...item,
        productName: getProductName(item.codProduct, lang),
        productNameEs: names.es,
        productNameEn: names.en,
      };
    });

    let topCorridors = db.getTopCorridors(8);
    let corridorMeta = db.getCorridorMeta();
    const typeInsights = db.getTodayTypeInsights();
    let historicalRaw;
    let cacheMeta: {
      source: "cache" | "live";
      windowHours: number;
      generatedAt: string;
      ageSeconds: number;
      customRange: boolean;
    };

    if (hasCustomRange) {
      const customSince = rawFrom ?? Math.max(0, (rawTo ?? nowEpoch) - historyHours * 3600);
      const customUntil = rawTo ?? nowEpoch;
      const safeSince = Math.max(0, Math.min(customSince, customUntil));
      const safeUntil = Math.max(safeSince + 1, Math.min(customUntil, nowEpoch));

      if (rawWindowStart !== null) {
        if (safeUntil <= rawWindowStart) {
          historyPrecisionMode = "aggregated_only";
        } else if (safeSince < rawWindowStart) {
          historyPrecisionMode = "mixed";
        } else {
          historyPrecisionMode = "raw_detail";
        }
      }

      historicalRaw = db.getHistoricalStatsCustom(safeSince, safeUntil, {
        preferAggregated: historyPrecisionMode !== "raw_detail",
      });
      cacheMeta = {
        source: "live",
        windowHours: historyHours,
        generatedAt: new Date(nowEpoch * 1000).toISOString(),
        ageSeconds: 0,
        customRange: true,
      };
    } else if (dashboardPresetCache.isPresetHours(historyHours)) {
      const cached = await dashboardPresetCache.read(historyHours);
      const cachedHasAccountability = Boolean(
        (cached?.historical as { accountability?: unknown } | undefined)?.accountability,
      );
      if (cached && cachedHasAccountability) {
        historicalRaw = cached.historical;
        topCorridors = Array.isArray(cached.topCorridors) ? cached.topCorridors : db.getTopCorridors(8);
        corridorMeta = cached.corridorMeta ?? db.getCorridorMeta();
        if (rawWindowStart !== null && historyHours * 3600 > rawWindowSeconds) {
          historyPrecisionMode = "aggregated_only";
        }
        cacheMeta = {
          source: "cache",
          windowHours: historyHours,
          generatedAt: cached.generatedAt,
          ageSeconds: Math.max(0, nowEpoch - cached.generatedAtEpoch),
          customRange: false,
        };
      } else {
        const regenerated = await dashboardPresetCache.generate(historyHours, nowEpoch);
        historicalRaw = regenerated.historical;
        topCorridors = regenerated.topCorridors ?? db.getTopCorridors(8);
        corridorMeta = regenerated.corridorMeta ?? db.getCorridorMeta();
        if (rawWindowStart !== null && historyHours * 3600 > rawWindowSeconds) {
          historyPrecisionMode = "aggregated_only";
        }
        cacheMeta = {
          source: "live",
          windowHours: historyHours,
          generatedAt: new Date(nowEpoch * 1000).toISOString(),
          ageSeconds: 0,
          customRange: false,
        };
      }
    } else {
      historicalRaw = db.getHistoricalStats(historyHours);
      if (rawWindowStart !== null && historyHours * 3600 > rawWindowSeconds) {
        historyPrecisionMode = "aggregated_only";
      }
      cacheMeta = {
        source: "live",
        windowHours: historyHours,
        generatedAt: new Date(nowEpoch * 1000).toISOString(),
        ageSeconds: 0,
        customRange: false,
      };
    }

    const historicalByProduct = (historicalRaw.byProduct as Array<{ cod_product: number }>).map(
      (item) => {
        const names = getProductNames(item.cod_product);
        return {
          ...item,
          codProduct: item.cod_product,
          productName: getProductName(item.cod_product, lang),
          productNameEs: names.es,
          productNameEn: names.en,
        };
      },
    );
    const historicalAccountabilityRaw = historicalRaw.accountability ?? {
      summary: {
        delayed15Pct: 0,
        severe60Pct: 0,
        observations: historicalRaw.summary?.observations ?? 0,
        uniqueTrains: historicalRaw.summary?.uniqueTrains ?? 0,
        worseningVs7d: {
          trend: "flat",
          current24hDelayed15Pct: 0,
          baseline7dDelayed15Pct: 0,
          deltaDelayed15Pct: 0,
          current24hSevere60Pct: 0,
          baseline7dSevere60Pct: 0,
          deltaSevere60Pct: 0,
        },
      },
      criticalRoutes: [],
      repeatOffenders: [],
    };
    const accountabilityRepeatOffenders = Array.isArray(historicalAccountabilityRaw.repeatOffenders)
      ? historicalAccountabilityRaw.repeatOffenders.map(
          (item: {
            codProduct?: number;
          }) => {
            const codProduct = Number(item.codProduct ?? 0);
            const productNames = getProductNames(codProduct);

            return {
              ...item,
              productName: getProductName(codProduct, lang),
              productNameEs: productNames.es,
              productNameEn: productNames.en,
            };
          },
        )
      : [];

    const historicalAccountability = {
      summary: historicalAccountabilityRaw.summary,
      criticalRoutes: Array.isArray(historicalAccountabilityRaw.criticalRoutes)
        ? historicalAccountabilityRaw.criticalRoutes
        : [],
      repeatOffenders: accountabilityRepeatOffenders,
    };
    const topDelayed = db.listTrains({
      query: null,
      minDelay: null,
      limit: 12,
      offset: 0,
    });

    return json({
      ok: true,
      language: lang,
      generatedAt: new Date().toISOString(),
      cacheMeta,
      historyPrecision: {
        mode: historyPrecisionMode,
        rawWindowDays,
        archivesAvailable: config.archiveEnabled,
      },
      corridorMeta,
      overview: {
        ...overview,
        lastSeenAtIso: toIso(overview.lastSeenAt),
        accessiblePct:
          overview.activeTrains > 0
            ? Number(((overview.accessibleCount / overview.activeTrains) * 100).toFixed(1))
            : 0,
      },
      today: db.getTodayAggregate(),
      typeInsights: {
        problematic: typeInsights.problematic
          ? {
              ...typeInsights.problematic,
              productName: getProductName(typeInsights.problematic.codProduct, lang),
              productNameEs: getProductName(typeInsights.problematic.codProduct, "es"),
              productNameEn: getProductName(typeInsights.problematic.codProduct, "en"),
            }
          : null,
        volume: typeInsights.volume
          ? {
              ...typeInsights.volume,
              productName: getProductName(typeInsights.volume.codProduct, lang),
              productNameEs: getProductName(typeInsights.volume.codProduct, "es"),
              productNameEn: getProductName(typeInsights.volume.codProduct, "en"),
            }
          : null,
      },
      historical: {
        hours: historicalRaw.hours,
        sinceEpoch: historicalRaw.sinceEpoch,
        nowEpoch: historicalRaw.nowEpoch,
        untilEpoch: historicalRaw.untilEpoch ?? historicalRaw.nowEpoch,
        customRange: hasCustomRange,
        summary: historicalRaw.summary,
        topProblematicProduct: historicalRaw.topProblematicProduct
          ? {
              ...historicalRaw.topProblematicProduct,
              productName: getProductName(historicalRaw.topProblematicProduct.codProduct, lang),
              productNameEs: getProductName(historicalRaw.topProblematicProduct.codProduct, "es"),
              productNameEn: getProductName(historicalRaw.topProblematicProduct.codProduct, "en"),
            }
          : null,
        topProblematicCorridor: historicalRaw.topProblematicCorridor,
        accountability: historicalAccountability,
        byProduct: historicalByProduct,
        dailyTrend: historicalRaw.dailyTrend,
        ingestion: historicalRaw.ingestion,
      },
      delayBuckets,
      byProduct,
      topCorridors,
      topDelayed: withProductLabels(topDelayed as Array<{ cod_product: number }>, lang),
      recentRuns: db.getRecentRuns(12),
      ingestor: ingestor.getStatus(),
      api: {
        docs: "/api/docs",
        raw: "/api/raw/live",
      },
    });
  }

  if (url.pathname === "/api/ops") {
    const windowRaw = parseIntOrNull(url.searchParams.get("window")) ?? 168;
    const windowHours = [24, 168, 720].includes(windowRaw) ? windowRaw : 168;
    const nowEpoch = Math.floor(Date.now() / 1000);
    let payload = db.getOpsMetrics(windowHours);
    let cacheMeta: {
      source: "cache" | "live";
      windowHours: number;
      generatedAt: string;
      ageSeconds: number;
    } = {
      source: "live",
      windowHours,
      generatedAt: payload.generatedAt,
      ageSeconds: 0,
    };

    if (opsPresetCache.isPresetHours(windowHours)) {
      const cached = await opsPresetCache.read(windowHours);
      if (cached) {
        payload = cached;
        cacheMeta = {
          source: "cache",
          windowHours,
          generatedAt: cached.generatedAt,
          ageSeconds: Math.max(0, nowEpoch - cached.generatedAtEpoch),
        };
      } else {
        payload = await opsPresetCache.generate(windowHours);
        cacheMeta = {
          source: "live",
          windowHours,
          generatedAt: payload.generatedAt,
          ageSeconds: 0,
        };
      }
    }

    const localizeOpsRank = (item: {
      key: string;
      label: string;
      services: number;
      p15: number;
      p60: number;
      p95PeakDelay: number;
      tm60Sec: number;
      tm60Per100ServicesSec: number;
      tm60Per10000KmSec: number;
      kmObserved: number;
    }) => {
      const maybeProduct = Number(item.key);
      if (Number.isInteger(maybeProduct) && String(maybeProduct) === item.key) {
        const names = getProductNames(maybeProduct);
        return {
          ...item,
          productName: getProductName(maybeProduct, lang),
          productNameEs: names.es,
          productNameEn: names.en,
        };
      }

      return item;
    };

    return json({
      ok: true,
      language: lang,
      generatedAt: new Date().toISOString(),
      cacheMeta,
      summary: payload.summary,
      rankings: {
        corridorDirection: {
          risk: payload.rankings.corridorDirection.risk,
          severity: payload.rankings.corridorDirection.severity,
          load: payload.rankings.corridorDirection.load,
        },
        product: {
          risk: payload.rankings.product.risk.map(localizeOpsRank),
          severity: payload.rankings.product.severity.map(localizeOpsRank),
          load: payload.rankings.product.load.map(localizeOpsRank),
        },
      },
      confidence: payload.confidence,
    });
  }

  if (url.pathname === "/api/trains") {
    const query = url.searchParams.get("q");
    const minDelay = parseIntOrNull(url.searchParams.get("minDelay"));
    const limit = Math.min(Math.max(parseIntOrNull(url.searchParams.get("limit")) ?? 50, 1), 200);
    const offset = Math.max(parseIntOrNull(url.searchParams.get("offset")) ?? 0, 0);

    const rows = db.listTrains({
      query,
      minDelay,
      limit,
      offset,
    });

    const total = db.countTrains(query, minDelay);

    return json({
      ok: true,
      language: lang,
      total,
      limit,
      offset,
      items: withProductLabels(rows as Array<{ cod_product: number }>, lang),
    });
  }

  if (url.pathname.startsWith("/api/trains/") && url.pathname.endsWith("/history")) {
    const rawId = url.pathname.replace("/api/trains/", "").replace("/history", "");
    const codComercial = rawId.trim();

    if (!/^\d{5}$/.test(codComercial)) {
      return asError(lang, "invalidTrainCode", "INVALID_TRAIN_CODE", 400);
    }

    const hours = Math.min(Math.max(parseIntOrNull(url.searchParams.get("hours")) ?? 24, 1), 168);
    const history = db.getTrainHistory(codComercial, hours);

    return json({
      ok: true,
      language: lang,
      codComercial,
      hours,
      snapshots: history.snapshots,
      observations: history.observations,
      daily: history.daily,
    });
  }

  if (url.pathname === "/api/ingestion/runs") {
    const limit = Math.min(Math.max(parseIntOrNull(url.searchParams.get("limit")) ?? 50, 1), 200);

    return json({
      ok: true,
      language: lang,
      items: db.getRecentRuns(limit),
      ingestor: ingestor.getStatus(),
    });
  }

  if (url.pathname === "/api/raw/live") {
    const cachedPayload = await readCachedRawPayload();
    const trains = db.listTrains({
      query: null,
      minDelay: null,
      limit: config.rawMaxTrains,
      offset: 0,
    });

    const storage = db.getObservationArchiveStorage();

    return json({
      ok: true,
      language: lang,
      generatedAt: new Date().toISOString(),
      source: {
        endpointList: config.endpointList,
        cacheFile: config.cacheFile,
        cacheAvailable: cachedPayload !== null,
      },
      limits: {
        maxConcurrentRequestsPerKey: 1,
        minIntervalMs: config.apiRateLimitMs,
      },
      ingestor: ingestor.getStatus(),
      overview: db.getOverview(),
      delayBuckets: db.getDelayBuckets(),
      today: db.getTodayAggregate(),
      historyCoverage: db.getHistoryCoverage(48, Math.floor(config.pollingIntervalMs / 1000)),
      storage,
      recentRuns: db.getRecentRuns(30),
      trainsCurrent: withProductLabels(trains as Array<{ cod_product: number }>, lang),
      rawPayload: cachedPayload,
    });
  }

  if (url.pathname === "/api/history/coverage") {
    const hours = Math.min(Math.max(parseIntOrNull(url.searchParams.get("hours")) ?? 48, 1), 720);

    return json({
      ok: true,
      language: lang,
      report: db.getHistoryCoverage(hours, Math.floor(config.pollingIntervalMs / 1000)),
      storage: db.getObservationArchiveStorage(),
    });
  }

  if (url.pathname === "/api/history/recover") {
    if (request.method !== "POST" && request.method !== "GET") {
      return json(
        {
          ok: false,
          code: "METHOD_NOT_ALLOWED",
          language: lang,
          message:
            lang === "es"
              ? "Metodo no permitido. Usa GET o POST."
              : "Method not allowed. Use GET or POST.",
        },
        { status: 405 },
      );
    }

    const hours = Math.min(Math.max(parseIntOrNull(url.searchParams.get("hours")) ?? 48, 1), 720);
    const sinceEpoch = Math.floor(Date.now() / 1000) - hours * 3600;
    const recovered = db.recoverObservationsFromSnapshots(sinceEpoch);
    const report = db.getHistoryCoverage(hours, Math.floor(config.pollingIntervalMs / 1000));

    return json({
      ok: true,
      language: lang,
      recovered,
      report,
    });
  }

  return asError(lang, "notFound", "NOT_FOUND", 404);
};

const readCachedRawPayload = async () => {
  const file = Bun.file(config.cacheFile);

  if (!(await file.exists())) {
    return null;
  }

  try {
    return await file.json();
  } catch {
    return null;
  }
};

const serveStatic = async (pathname: string): Promise<Response> => {
  const target = pathname === "/" ? "/index.html" : pathname;
  const normalized = normalize(target).replace(/^\/+/, "");

  if (normalized.includes("..")) {
    return new Response("Forbidden", { status: 403 });
  }

  const absolutePath = join(publicRoot, normalized);
  const file = Bun.file(absolutePath);

  if (await file.exists()) {
    const extension = extname(absolutePath).toLowerCase();
    const cacheControl =
      extension === ".html" ? "no-cache" : "public, max-age=31536000, immutable";

    return new Response(file, {
      headers: {
        "cache-control": cacheControl,
      },
    });
  }

  return new Response("Not found", { status: 404 });
};
