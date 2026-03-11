# retrasometro (Bun + SQLite)

Dashboard de métricas ferroviarias en tiempo real con ingesta automática cada minuto (UI en español e inglés).

## Índice rápido

- [Parte 1: Información General](#parte-1)
- [Qué es](#que-es)
- [Objetivo](#objetivo)
- [Qué incluye](#que-incluye)
- [Alcance del histórico](#alcance-del-historico)
- [Transparencia y publicación de datos](#transparencia-publicacion)
- [Esquema de datos](#esquema-datos)
- [Parte 2: Guía Técnica](#parte-2)
- [Requisitos](#requisitos)
- [Instalación local rápida](#instalacion-rapida)
- [Instalación local paso a paso](#instalacion-paso-a-paso)
- [Variables de entorno](#variables-entorno)
- [Endpoints API](#endpoints-api)
- [Flujo de consumo API](#flujo-consumo-api)
- [Ejemplo de rango histórico](#ejemplo-rango-historico)
- [Recuperación de histórico](#recuperacion-historico)
- [Docker + Postgres](#docker-postgres)
- [Migración manual SQLite -> Postgres](#migracion-manual)

<a id="parte-1"></a>
## Parte 1: Información General

<a id="que-es"></a>
### Qué es

`retrasometro` es una aplicación para monitorizar trenes en tiempo real y construir un histórico consultable de su comportamiento (retrasos, actividad, cobertura y tendencias).

<a id="objetivo"></a>
### Objetivo

El objetivo principal es **almacenar y mantener un histórico trazable de datos públicos de circulación de trenes** para facilitar análisis y transparencia operativa a lo largo del tiempo.

<a id="que-incluye"></a>
### Qué incluye

- Ingesta de `flotaLD.json` cada 60 segundos (configurable).
- Fallback robusto:
  - reintento por lista de endpoints.
  - caché local (`data/cache/flotaLD.latest.json`) si falla la red.
- Almacenamiento histórico optimizado:
  - `trains_current`: estado actual de trenes activos.
  - `train_snapshots`: snapshots compactados (solo cambios o heartbeat).
  - `train_observations`: histórico completo por ciclo de ingesta.
  - `train_daily_stats`: acumulados diarios por tren.
- Limpieza y compactación automática:
  - elimina trenes no vistos tras un umbral (`STALE_TRAIN_SECONDS`).
  - purga snapshots antiguos (`SNAPSHOT_RETENTION_HOURS`).
  - histórico configurable (`HISTORY_RETENTION_DAYS`, `0` = sin purga).
  - conserva acumulados diarios.
- Frontend + API:
  - selector de idioma ES/EN.
  - documentación API tipo Swagger con ejemplos reales.
  - vista de datos en bruto en tiempo real (`/api/raw/live`).
  - ventanas históricas (24h / 7d / 30d) y rango personalizado.
- Seguridad de API:
  - clave temporal obligatoria para rutas de datos.
  - máximo 1 petición concurrente por clave.
  - máximo 1 petición por segundo por clave.

<a id="alcance-del-historico"></a>
### Alcance del histórico (importante)

- La app puede reconstruir el pasado **solo dentro de lo ya capturado en tu base**.
- Si nunca se guardó una ventana temporal concreta, no se puede reconstruir al 100% desde el endpoint público (que entrega estado actual, no histórico completo).

<a id="transparencia-publicacion"></a>
### Transparencia y publicación de datos

Este repositorio permite publicar `data/renfe.db` porque los datos provienen de fuentes públicas.

Comandos típicos:

```bash
git add data/renfe.db
git commit -m "Publicar snapshot de base de datos"
git push
```

Nota: si la base crece mucho, considera snapshots periódicos o Git LFS.

<a id="esquema-datos"></a>
### Esquema de datos (resumen)

- `trains_current`: vista viva de trenes.
- `train_snapshots`: historial de cambios por tren.
- `train_observations`: historial temporal por ingesta (incluye `source` e `is_estimated`).
- `train_daily_stats`: acumulado por día y tren.
- `ingestion_runs`: auditoría de cada ciclo de ingesta.
- `stations`: catálogo de estaciones (`estaciones.geojson`).

<a id="parte-2"></a>
## Parte 2: Guía Técnica (Instalación y Operación)

<a id="requisitos"></a>
### Requisitos

- Bun 1.3+
- Docker (opcional)
- Docker Compose (opcional)

<a id="instalacion-rapida"></a>
### Instalación local rápida

```bash
bun install
bun run start
```

Abre: `http://localhost:3000`

Modo desarrollo:

```bash
bun run dev
```

<a id="instalacion-paso-a-paso"></a>
### Instalación local paso a paso

1. Abre terminal en la carpeta del proyecto.
2. Instala dependencias:
   - `bun install`
3. (Opcional) copia variables de entorno:
   - `cp .env.example .env`
4. Arranca la app:
   - `bun run start`
5. Abre en navegador:
   - `http://localhost:3000`

<a id="variables-entorno"></a>
### Variables de entorno

Copia `.env.example` a `.env` y ajusta si quieres:

```bash
cp .env.example .env
```

Variables principales:

- `PORT`: puerto HTTP (default `3000`).
- `DB_PATH`: ruta SQLite (default `./data/renfe.db`).
- `POLL_INTERVAL_MS`: frecuencia de ingesta (default `60000`).
- `RENFE_ENDPOINTS`: endpoints separados por coma.
- `FETCH_TIMEOUT_MS`: timeout HTTP por intento.
- `STALE_TRAIN_SECONDS`: segundos para purgar trenes no vistos.
- `SNAPSHOT_RETENTION_HOURS`: retención de snapshots.
- `SNAPSHOT_HEARTBEAT_SECONDS`: inserta snapshot aunque no cambie el tren cada N segundos.
- `COMPACT_EVERY_RUNS`: cada cuántas ingestas ejecutar compactación.
- `API_KEY_TTL_SECONDS`: duración de la clave API temporal.
- `API_RATE_LIMIT_MS`: intervalo mínimo entre peticiones por clave (default `1000`).
- `RAW_MAX_TRAINS`: máximo de trenes en `/api/raw/live`.
- `HISTORY_RETENTION_DAYS`: retención de `train_observations` (0 = indefinido).
- `RECOVERY_LOOKBACK_HOURS`: ventana de recuperación desde snapshots al arrancar.
- `POSTGRES_URL`: conexión para migrar SQLite -> Postgres.
- `SQLITE_PATH`: ruta SQLite para script de migración.
- `MIGRATION_BATCH_SIZE`: lote de inserción en migración.
- `AUTO_MIGRATE_PG_ON_START`: migración automática a Postgres al arrancar Docker (`1`/`0`).
- `BOOTSTRAP_SQLITE_PATH`: ruta de bootstrap de SQLite al primer arranque Docker.
- `BOOTSTRAP_CACHE_PATH`: ruta de bootstrap de cache al primer arranque Docker.

<a id="endpoints-api"></a>
### Endpoints API

Públicos:

- `GET /api/docs`
- `GET /api/health`
- `GET /api/auth/request-key`

Protegidos (requieren `x-api-key`):

- `GET /api/dashboard?historyHours=24|168|720&historyFrom=ISO|epoch&historyTo=ISO|epoch`
- `GET /api/trains?q=&minDelay=&limit=&offset=`
- `GET /api/trains/:codComercial/history?hours=24`
- `GET /api/ingestion/runs?limit=50`
- `GET /api/raw/live`
- `GET /api/history/coverage?hours=48`
- `POST /api/history/recover?hours=48`

Idioma de API:

- sin parámetro `lang` en URL.
- usa cabecera `Accept-Language` (`es` o `en`).

<a id="flujo-consumo-api"></a>
### Flujo de consumo API

1. Solicita clave temporal:
   - `GET /api/auth/request-key`
2. Usa la clave en cabecera:
   - `x-api-key: <tu_clave>`
3. Respeta límites:
   - 1 petición en curso por clave.
   - 1 petición/segundo por clave.

<a id="ejemplo-rango-historico"></a>
### Ejemplo: rango histórico personalizado

```bash
KEY=$(curl -s 'http://localhost:3000/api/auth/request-key' | jq -r '.apiKey')
curl -s -H "x-api-key: $KEY" \\
  -H "Accept-Language: es" \\
  "http://localhost:3000/api/dashboard?historyFrom=2026-03-01T00:00:00Z&historyTo=2026-03-10T23:59:59Z" | jq .
```

También acepta epoch segundos:

```bash
curl -s -H "x-api-key: $KEY" \\
  "http://localhost:3000/api/dashboard?historyFrom=1772582400&historyTo=1773359999" | jq .
```

<a id="recuperacion-historico"></a>
### Recuperación de histórico

- Si faltan observaciones, puedes recuperar parcialmente desde `train_snapshots`:
  - `POST /api/history/recover?hours=48`
- Verifica cobertura real y huecos:
  - `GET /api/history/coverage?hours=48`

<a id="docker-postgres"></a>
### Docker + Postgres

Levantar app + Postgres:

```bash
docker compose up -d --build
```

Servicios:

- `app` en `http://localhost:3000`
- `postgres` en `localhost:5432` (`renfe/renfe`, DB `renfe_analytics`)

Persistencia entre sesiones:

- `appdata` para SQLite + cache (`/app/data`)
- `pgdata` para Postgres (`/var/lib/postgresql/data`)

Integración automática al arrancar Docker:

1. Si `appdata` está vacío y existe `./data/renfe.db`, se importa automáticamente.
2. Si existe `./data/cache/flotaLD.latest.json`, también se importa.
3. Si `AUTO_MIGRATE_PG_ON_START=1`, se ejecuta migración automática SQLite -> Postgres.

Ver volúmenes:

```bash
docker volume ls | grep -E "appdata|pgdata"
docker volume inspect "$(docker volume ls -q | grep appdata | head -n1)"
docker volume inspect "$(docker volume ls -q | grep pgdata | head -n1)"
```

<a id="migracion-manual"></a>
### Migración manual de SQLite a Postgres

Con la base SQLite actual en `./data/renfe.db`:

```bash
POSTGRES_URL=postgresql://renfe:renfe@localhost:5432/renfe_analytics \\
SQLITE_PATH=./data/renfe.db \\
bun run migrate:pg
```

Con Docker Compose:

```bash
docker compose --profile tools run --rm migrate_to_postgres
```

El script crea esquema en Postgres, vacía tablas destino y copia todas las tablas históricas.

---

Este proyecto consume endpoints públicos desde frontend oficial de RENFE y está pensado para analítica/monitorización técnica.
