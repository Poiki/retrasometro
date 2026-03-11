import type { AppLanguage } from "./products";

type MessageKey =
  | "missingApiKey"
  | "invalidApiKey"
  | "expiredApiKey"
  | "requestInProgress"
  | "rateLimited"
  | "invalidTrainCode"
  | "notFound"
  | "keyIssued"
  | "healthOk"
  | "docsTitle"
  | "docsSecurity"
  | "docsRequestKey"
  | "docsProtected"
  | "docsRawEndpoint";

const MESSAGES: Record<AppLanguage, Record<MessageKey, string>> = {
  es: {
    missingApiKey: "Falta la clave API temporal en la cabecera x-api-key.",
    invalidApiKey: "Clave API no valida.",
    expiredApiKey: "La clave API temporal ha caducado. Solicita una nueva.",
    requestInProgress: "Ya hay una peticion en curso con esta clave.",
    rateLimited: "Solo se permite 1 peticion por segundo con esta clave.",
    invalidTrainCode: "codComercial invalido. Debe tener 5 digitos.",
    notFound: "Recurso no encontrado.",
    keyIssued: "Clave temporal emitida correctamente.",
    healthOk: "Servicio operativo.",
    docsTitle: "API de retrasometro",
    docsSecurity: "Todas las rutas de datos requieren clave temporal x-api-key.",
    docsRequestKey: "Solicita clave en /api/auth/request-key.",
    docsProtected: "Rutas protegidas: /api/dashboard, /api/trains, /api/trains/:id/history, /api/ingestion/runs, /api/raw/live.",
    docsRawEndpoint: "Endpoint de datos en bruto: /api/raw/live.",
  },
  en: {
    missingApiKey: "Missing temporary API key in x-api-key header.",
    invalidApiKey: "Invalid API key.",
    expiredApiKey: "Temporary API key expired. Request a new one.",
    requestInProgress: "There is already an in-flight request using this key.",
    rateLimited: "Only 1 request per second is allowed for this key.",
    invalidTrainCode: "Invalid codComercial. Must contain 5 digits.",
    notFound: "Resource not found.",
    keyIssued: "Temporary key issued successfully.",
    healthOk: "Service healthy.",
    docsTitle: "retrasometro API",
    docsSecurity: "All data routes require temporary x-api-key.",
    docsRequestKey: "Request key at /api/auth/request-key.",
    docsProtected: "Protected routes: /api/dashboard, /api/trains, /api/trains/:id/history, /api/ingestion/runs, /api/raw/live.",
    docsRawEndpoint: "Raw data endpoint: /api/raw/live.",
  },
};

export const resolveLanguage = (url: URL, request: Request): AppLanguage => {
  const queryLang = url.searchParams.get("lang")?.toLowerCase();
  if (queryLang === "es" || queryLang === "en") {
    return queryLang;
  }

  const acceptLanguage = request.headers.get("accept-language")?.toLowerCase() ?? "";
  if (acceptLanguage.includes("en")) {
    return "en";
  }

  return "es";
};

export const t = (lang: AppLanguage, key: MessageKey): string => {
  return MESSAGES[lang][key];
};
