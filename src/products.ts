export type AppLanguage = "es" | "en";

const PRODUCT_MAP_ES: Record<number, string> = {
  0: "Movimientos",
  2: "AVE",
  3: "Avant",
  4: "Talgo",
  5: "Altaria",
  7: "Diurno",
  8: "Estrella",
  9: "Tren Hotel",
  10: "Euromed",
  11: "Alvia",
  13: "Intercity",
  14: "Andalucia Express",
  15: "Catalunya Express",
  16: "Media Distancia",
  18: "Regional",
  19: "Regional Express",
  20: "TRD",
  21: "Cercanias",
  22: "Trenes historicos",
  23: "Trenes turisticos",
  24: "AV City",
  25: "Ave Tgv",
  26: "Tranvia",
  27: "Euskotren",
  28: "Avlo",
  29: "Trenes tematicos",
};

const PRODUCT_MAP_EN: Record<number, string> = {
  0: "Movements",
  2: "AVE",
  3: "Avant",
  4: "Talgo",
  5: "Altaria",
  7: "Day Train",
  8: "Estrella",
  9: "Hotel Train",
  10: "Euromed",
  11: "Alvia",
  13: "Intercity",
  14: "Andalusia Express",
  15: "Catalonia Express",
  16: "Medium Distance",
  18: "Regional",
  19: "Regional Express",
  20: "TRD",
  21: "Commuter",
  22: "Historic Trains",
  23: "Tourist Trains",
  24: "AV City",
  25: "Ave Tgv",
  26: "Tram",
  27: "Euskotren",
  28: "Avlo",
  29: "Thematic Trains",
};

export const getProductName = (codProduct: number, lang: AppLanguage = "es"): string => {
  if (lang === "en") {
    return PRODUCT_MAP_EN[codProduct] ?? `Product ${codProduct}`;
  }

  return PRODUCT_MAP_ES[codProduct] ?? `Producto ${codProduct}`;
};

export const getProductNames = (codProduct: number) => {
  return {
    es: getProductName(codProduct, "es"),
    en: getProductName(codProduct, "en"),
  };
};
