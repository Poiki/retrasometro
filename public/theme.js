const THEME_STORAGE_KEY = "retrasometro_theme_mode";
const DEFAULT_THEME_MODE = "system";
const THEME_MEDIA_QUERY = "(prefers-color-scheme: dark)";
const VALID_THEME_MODES = new Set(["system", "light", "dark"]);

const safeStorageGet = (key) => {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
};

const safeStorageSet = (key, value) => {
  try {
    localStorage.setItem(key, value);
  } catch {
    // noop
  }
};

const normalizeThemeMode = (value) => {
  if (VALID_THEME_MODES.has(value)) {
    return value;
  }
  return DEFAULT_THEME_MODE;
};

export const getStoredThemeMode = () => {
  return normalizeThemeMode(safeStorageGet(THEME_STORAGE_KEY));
};

const resolveTheme = (mode, media) => {
  if (mode === "light" || mode === "dark") {
    return mode;
  }

  if (media) {
    return media.matches ? "dark" : "light";
  }

  return "light";
};

const applyResolvedTheme = (resolvedTheme) => {
  document.documentElement.setAttribute("data-theme", resolvedTheme);
  document.documentElement.style.colorScheme = resolvedTheme;
};

export const initThemeController = ({ selectEl = null, onThemeChange = null } = {}) => {
  const media = window.matchMedia ? window.matchMedia(THEME_MEDIA_QUERY) : null;
  let mode = getStoredThemeMode();
  let resolvedTheme = "light";

  const emitChange = () => {
    if (typeof onThemeChange === "function") {
      onThemeChange({
        mode,
        resolvedTheme,
      });
    }
  };

  const applyTheme = () => {
    resolvedTheme = resolveTheme(mode, media);
    applyResolvedTheme(resolvedTheme);
    emitChange();
  };

  const setMode = (nextMode) => {
    mode = normalizeThemeMode(nextMode);
    safeStorageSet(THEME_STORAGE_KEY, mode);

    if (selectEl && selectEl.value !== mode) {
      selectEl.value = mode;
    }

    applyTheme();
  };

  const onMediaChange = () => {
    if (mode === "system") {
      applyTheme();
    }
  };

  if (selectEl) {
    selectEl.value = mode;
    selectEl.addEventListener("change", () => {
      setMode(selectEl.value);
    });
  }

  if (media) {
    if (typeof media.addEventListener === "function") {
      media.addEventListener("change", onMediaChange);
    } else if (typeof media.addListener === "function") {
      media.addListener(onMediaChange);
    }
  }

  applyTheme();

  return {
    getMode: () => mode,
    getResolvedTheme: () => resolvedTheme,
    setMode,
    destroy: () => {
      if (media) {
        if (typeof media.removeEventListener === "function") {
          media.removeEventListener("change", onMediaChange);
        } else if (typeof media.removeListener === "function") {
          media.removeListener(onMediaChange);
        }
      }
    },
  };
};
