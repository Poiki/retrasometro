# Sistema de diseño de retrasometro

Referencia visual del panel público (`public/index.html`, `public/styles.css`, `public/app.js`). Las páginas secundarias (`api-docs.html`, `ops-lab.html`) comparten la hoja de estilos y heredan los mismos tokens.

## 1. Carácter visual

Panel de datos tipo SaaS moderno: fondo gris azulado muy claro, tarjetas blancas con esquinas redondeadas y sombra mínima, un único color de acento azul, iconos de línea y números grandes en semibold. La jerarquía se consigue con capas de color (fondo → tarjeta → subtarjeta), no con sombras ni bordes gruesos. Todo el estado semántico (retraso leve / medio / severo, mejora / empeora) se codifica con un pequeño conjunto de tintes: verde, ámbar, naranja, rojo.

Principios:

- Un solo acento: el azul `#1f5eff` marca interactivo, activo y marca. No hay segundo color de marca.
- Color = significado. Rojo y ámbar solo aparecen cuando un valor lo merece (retraso > 15 min, > 60 min, % de reincidencia alto). Una tarjeta neutra nunca lleva tinte.
- Densidad productiva: paneles compactos, tablas con filas de 36-40 px, texto base 15 px. Nada de márgenes de landing page.
- Sin librerías. Iconos SVG inline (sprite `<symbol>` en el `<head>` del body), gráfica SVG generada en JS, fuentes locales.

## 2. Tokens

Los nombres `--cds-*` se conservan por compatibilidad con `api-docs.html` y `ops-lab.html`. Los valores son los de esta paleta.

### Tema claro

| Token | Valor | Uso |
|---|---|---|
| `--cds-background` | `#f4f6fb` | Fondo de página |
| `--cds-layer-01` | `#ffffff` | Tarjetas, paneles, barra superior, pie |
| `--cds-layer-02` | `#f7f9fc` | Subtarjetas, cabeceras de tabla, cajas internas |
| `--cds-layer-hover-01` | `#f1f4f9` | Hover de filas y enlaces de navegación |
| `--cds-border-subtle` | `#e4e8f0` | Bordes de tarjeta, separadores de tabla |
| `--cds-text-primary` | `#0f172a` | Texto y cifras |
| `--cds-text-secondary` | `#5b6677` | Etiquetas, metadatos |
| `--cds-text-helper` | `#8b95a5` | Subtexto de tarjeta, ejes de gráfica |
| `--cds-link-primary` / `--cds-button-primary` | `#1f5eff` | Acento: botón primario, segmento activo, iconos, enlace |
| `--cds-button-primary-hover` | `#1a51db` | Hover del primario |
| `--accent-soft` | `#e8efff` | Fondo del icono de tarjeta, nav activo en móvil |
| `--cds-danger` / `--danger-soft` | `#dc2626` / `#fdecec` | Severo (> 60 min), tarjeta en rojo |
| `--cds-warning` / `--warning-soft` | `#d97706` / `#fff4e0` | Medio (16-60 min), eje derivado |
| `--cds-success` / `--success-soft` | `#16a34a` / `#e6f7ec` | En hora, "Datos en tiempo real", fuente oficial |
| `--neutral-soft` | `#eef2f7` | Fondo del control segmentado, pastilla "adelantado" |
| `--delay-mild` / `--delay-mild-bg` | `#b45309` / `#fff4d6` | Pastilla 1-15 min |
| `--delay-medium` / `--delay-medium-bg` | `#c2410c` / `#ffe8d6` | Pastilla 16-60 min |
| `--hero-from` → `--hero-to` | `#eef3ff` → `#dde7ff` | Degradado de la banda hero |
| `--shadow-card` | `0 1px 2px rgba(15,23,42,.04), 0 2px 8px rgba(15,23,42,.05)` | Única sombra de superficie |
| `--radius` / `--radius-sm` | `14px` / `10px` | Tarjetas y paneles / subtarjetas, botones, inputs |

### Tema oscuro (`[data-theme="dark"]`)

Fondo `#0b1220`, tarjeta `#121a2b`, subtarjeta `#182236`, borde `#243049`, texto `#e8edf6` / `#a7b2c4`, acento `#6b97ff` (botón primario `#2f6bff`). Los tintes semánticos pasan a versiones oscuras del mismo tono (`--danger-soft: #3a1518`, `--warning-soft: #3a2a08`, `--success-soft: #10301d`). El modo se resuelve en `theme.js` (sistema / claro / oscuro, persistido en `localStorage`) y se aplica antes de pintar con el script inline del `<head>`.

## 3. Tipografía

- Familia: IBM Plex Sans (300 / 400 / 600, woff2 locales). IBM Plex Mono solo para JSON y rutas de API en `api-docs`.
- Base: 15 px, line-height 1.5. Móvil: 14 px.
- Pesos: 400 para texto, 600 para títulos, cifras, código de tren y botones. No se usa 700.
- Escala:

| Rol | Tamaño | Peso |
|---|---|---|
| H1 hero | `clamp(1.75rem, 2.6vw, 2.25rem)`, letter-spacing -0.01em | 600 |
| H2 título de panel | 1.25rem | 600 |
| H3 título de subpanel | 1rem | 600 |
| Valor KPI | 1.625rem (compacto 1.25rem; valor textual 1.25rem) | 600, `tabular-nums` |
| Etiqueta KPI / celda | 0.8125rem | 400 |
| Subtexto, metadatos, cabecera de tabla | 0.75rem | 400 (cabecera 600) |
| Badges (`.source-badge`, `.method-tag`) | 0.66rem mayúsculas | 600 |

Cifras siempre con `font-variant-numeric: tabular-nums` y formateadas con `Intl.NumberFormat` del idioma activo.

## 4. Componentes

### Barra superior (`.topbar`)
Sticky, fondo blanco, borde inferior 1 px, 58 px de alto. Marca = icono de tren en cuadrado azul de 36 px radio 10 px + nombre en 1.25rem semibold. Navegación con icono 16 px + texto 0.875rem; el enlace activo va en azul con subrayado de 2 px pegado al borde inferior de la barra. A la derecha: selectores Idioma y Tema como `.control-field` (icono + etiqueta + `<select>` sin borde) y botón primario "API". En ≤ 1056 px la navegación baja a una segunda fila con scroll horizontal y las etiquetas de los selectores se ocultan.

### Banda hero (`.hero-band`)
Degradado azul claro, radio 14 px, 24 px × 28 px de padding. Izquierda: H1 + subtítulo. Derecha: pastilla verde "Datos en tiempo real" (punto con halo) y línea "Última actualización". Estado de error → pastilla roja. Decoración: SVG de vías en trazo azul al 14 % de opacidad, oculto en móvil.

### Tarjeta KPI (`.card`)
Flex horizontal: icono en caja de 38 px radio 10 px con fondo `--accent-soft` + cuerpo con etiqueta, valor y subtexto. Variantes:

- `.tone-danger` / `.tone-warning`: fondo tintado, sin borde, valor e icono en el color semántico. Se aplica automáticamente según el valor (retraso medio y máximo).
- `.kpi-icon.danger|warning|success`: solo tiñe el icono.
- `.progress`: barra de 6 px bajo el valor (accesibilidad).
- `.sub.accent` / `.sub.danger`: subtexto en azul o rojo semibold.
- `.kpi-grid.compact`: versión de 4 columnas con valor 1.25rem para el bloque histórico.

Rejilla: 5 columnas (10 tarjetas = 2 filas); 3 en ≤ 1312 px; 2 en ≤ 1056 px; 1 en ≤ 672 px.

### Pastilla de retraso (`.delay`)
Píldora de 68 px mínimo, 0.8125rem semibold: `ahead` gris, `ok` verde suave, `mild` amarillo suave, `medium` naranja suave, `severe` rojo sólido con texto blanco. Umbrales: < 0, 0, 1-15, 16-60, > 60 min.

### Botones (`.btn`)
38 px de alto, radio 10 px, 0.875rem. `.primary` azul; `.ghost` blanco con borde; `.sync` ámbar. Siempre icono 16 px + texto en un `<span>` con id propio para que el cambio de idioma no borre el icono.

### Campos
`.search-field` (lupa + input) y `.delay-field` (etiqueta + número + unidad) son contenedores con borde 1 px y radio 10 px; el foco pone borde azul y halo `rgba(31,94,255,.15)`. `.panel-search` es el filtro pequeño (34 px) de los subpaneles.

### Control segmentado (`.segmented`)
Fondo `--neutral-soft`, radio 10 px, padding 3 px. Botón activo azul sólido radio 8 px. Se usa para 24h / 7d / 30d. Al lado, `.range-chip` muestra "dd/mm/aaaa → dd/mm/aaaa" y despliega los inputs de rango personalizado.

### Paneles (`.panel`, `.subpanel`)
Panel: tarjeta blanca radio 14 px, padding 18 px × 20 px, `gap` 14 px. Subpanel: radio 10 px, padding 14 px × 16 px. Título con icono azul de 20 px (18 px en H3). `.findings` es la franja de hallazgos: fondo `--layer-02` con tres `.finding` (icono + etiqueta + titular + metadatos); el titular del corredor va en azul y la tendencia en un `.trend-badge` verde/rojo/gris.

### Tablas
Cabecera en `--layer-02`, 0.75rem semibold; celdas 0.8125rem con `tabular-nums`; hover de fila en `--layer-hover-01`. `.data-table` (trenes activos) tiene cabecera sticky y el contenedor limita a 600 px de alto. `.mini-table` (reincidencias) usa `td.rank` azul, `td.num` alineado a la derecha y `td.bad` (≥ 50 %) rojo / `td.warn` (≥ 20 %) ámbar.

### Barras (`.bar-row`)
Etiqueta + pista de 8 px redondeada + valor (+ columna extra en `.bars.four`). Relleno azul por defecto; `warning`, `danger`, `ok`, `muted` según el dato.

### Gráfica de tendencia (`.trend-svg`)
SVG `viewBox 640×240` generado en `renderTrendChart`: rejilla de 5 líneas, área y línea del pico (punteada, 12 % de opacidad) debajo del área y línea del retraso medio (22 % / trazo 2.5 px), puntos con `<title>` cuando hay ≤ 16 valores, etiquetas dd/mm. Serie: `historical.dailyTrend` (un punto por día).

### Pie (`.footer`)
Fondo blanco, borde superior, marca pequeña + eslogan, enlaces (API, Documentación, Metodología, Código) y estado con punto verde replicando "Última actualización".

## 5. Layout y espaciado

- Contenedor: 1480 px máx., padding 20 px (12 px en móvil).
- Separación entre secciones y tarjetas: 16 px (`--surface-gap`); dentro de tarjeta 12-14 px.
- Orden de la página: barra → hero → 10 KPI → trenes activos → 4 paneles en vivo (`.live-grid`, 4 col → 2 → 1) → estadísticas históricas (8 KPI compactos → hallazgos → `.hist-grid` 2 × 2: gráfica, distribución por tipo, rutas reincidentes, trenes reincidentes) → pie.
- Breakpoints: 1312 px, 1056 px, 672 px.

## 6. Profundidad

| Nivel | Tratamiento |
|---|---|
| Página | `--cds-background` plano |
| Tarjeta / panel | `--layer-01` + borde 1 px + `--shadow-card` |
| Subtarjeta, cabecera de tabla, hallazgos | `--layer-02`, borde 1 px, sin sombra |
| Flotante (popover de ayuda) | `--layer-01` + `--cds-shadow-float` |

No se añaden más sombras. La barra superior es sticky y solo lleva borde inferior.

## 7. Haz / no hagas

Haz:
- Pasar cada estado semántico por los tokens `--danger`, `--warning`, `--success` y sus `-soft`.
- Mantener los ids que `app.js` rellena (`#overview-cards`, `#trains-body`, `#historical-cards`, `#trend-chart`, `#accountability-routes`…) y los `<span id>` de texto traducible.
- Usar `tabular-nums` y `Intl.NumberFormat` para cualquier cifra.
- Mostrar solo datos que devuelve `/api/dashboard`; un subtexto se omite antes que inventarse (no hay deltas "vs. hora anterior" porque la API no los calcula).

No hagas:
- Añadir un segundo color de acento, degradados fuertes ni sombras en iconos o texto.
- Tintar una tarjeta sin que el valor supere un umbral.
- Cargar fuentes o iconos externos; el sprite SVG y las Plex locales cubren todo.
- Esquinas a 0 px: todo lo interactivo lleva radio 8-10 px y las tarjetas 14 px.

## 8. Guía para prompts

Colores rápidos: fondo `#f4f6fb`, tarjeta `#ffffff`, borde `#e4e8f0`, texto `#0f172a` / `#5b6677`, acento `#1f5eff` (suave `#e8efff`), rojo `#dc2626` / `#fdecec`, ámbar `#d97706` / `#fff4e0`, verde `#16a34a` / `#e6f7ec`.

Ejemplos:
- "Tarjeta KPI: fondo blanco, radio 14 px, borde 1 px `#e4e8f0`, padding 16 px; icono de línea en caja 38 px radio 10 px fondo `#e8efff` color `#1f5eff`; etiqueta 0.8125rem `#5b6677`; valor 1.625rem semibold `#0f172a` tabular; subtexto 0.75rem `#8b95a5`."
- "Pastilla de retraso severo: píldora, fondo `#dc2626`, texto blanco 0.8125rem semibold, `+78 min`."
- "Control segmentado: contenedor `#eef2f7` radio 10 px padding 3 px; opción activa `#1f5eff` texto blanco radio 8 px, 32 px de alto."
- "Barra superior sticky blanca 58 px, borde inferior `#e4e8f0`; marca con icono de tren en cuadrado azul; enlace activo azul con subrayado 2 px."
