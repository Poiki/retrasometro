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
- [Conservación, actualización y verificación](#conservacion-actualizacion)
- [Operación del servicio y restauración](#operacion-servicio)
- [Resultados de la mejora](STORAGE_REVIEW.md)

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
  - conserva snapshots, observaciones, lotes, ejecuciones y agregados sin límite por defecto.
  - permite retenciones explícitas; las observaciones solo se retiran tras archivado verificado.
  - la compactación física con `VACUUM` se ejecuta mediante el script de mantenimiento.
- Frontend + API:
  - selector de idioma ES/EN.
  - mapa interactivo con las últimas posiciones GPS, colores por retraso y detalle por tren; comparte los filtros de la tabla y muestra todos los resultados, sin su límite de filas.
  - documentación API tipo Swagger con ejemplos reales.
  - vista de datos en bruto en tiempo real (`/api/raw/live`).
  - ventanas históricas (24h / 7d / 30d) y rango personalizado.
- Seguridad de API:
  - clave temporal obligatoria para rutas de datos.
  - máximo 1 petición concurrente por clave.
  - intervalo mínimo por clave: `API_RATE_LIMIT_MS` (200 ms por defecto; 1.000 ms en Docker Compose).

<a id="alcance-del-historico"></a>
### Alcance del histórico (importante)

- La app puede reconstruir el pasado **solo dentro de lo ya capturado en tu base**.
- Si nunca se guardó una ventana temporal concreta, no se puede reconstruir al 100% desde el endpoint público (que entrega estado actual, no histórico completo).

<a id="transparencia-publicacion"></a>
### Transparencia y publicación de datos

Git conserva código, configuración de ejemplo, documentación y resultados de verificación. `data/` y `.env` están excluidos: SQLite, archivos históricos y backups se mantienen en el almacenamiento del servicio.

Para distribuir los datos, usa una instantánea coherente de SQLite junto con los archivos del manifiesto. La API de backup de SQLite incorpora las transacciones confirmadas del WAL; una copia aislada de `renfe.db` mientras la ingesta está activa puede omitirlas. El paquete de mantenimiento contiene también `.env`, por lo que debe mantenerse privado.

<a id="esquema-datos"></a>
### Esquema de datos (resumen)

- `trains_current`: vista viva de trenes.
- `train_snapshots`: historial de cambios por tren.
- `train_observations`: historial temporal por ingesta (incluye `source` e `is_estimated`).
- `train_daily_stats`: acumulado por día y tren.
- `train_hourly_train_stats`: agregados horarios para consultas históricas.
- `ingestion_batches`: procedencia de cada lote de observaciones.
- `observation_archive_chunks`: rutas, rangos, recuentos y SHA-256 del histórico comprimido.
- `ops_service_hourly` y `ops_materialization_state`: métricas operativas y avance de materialización.
- `ingestion_runs`: auditoría de cada ciclo de ingesta.
- `stations`: catálogo de estaciones (`estaciones.geojson`).
- `app_state`: última marca del proveedor y estado de recuperación histórica.

<a id="parte-2"></a>
## Parte 2: Guía Técnica (Instalación y Operación)

<a id="requisitos"></a>
### Requisitos

- Bun 1.3+
- Zstd (`zstd`) para leer o crear archivos históricos y ejecutar las comprobaciones de regresión.
- Python 3.11+ y systemd solo para el mantenimiento del servicio local.
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
- `STATIONS_ENDPOINT`: fuente principal GeoJSON de estaciones (larga distancia).
- `STATIONS_SUPPLEMENTAL_ENDPOINT`: fuente secundaria GeoJSON (p. ej. Cercanías) para completar códigos faltantes.
- `FETCH_TIMEOUT_MS`: timeout HTTP por intento.
- `STALE_TRAIN_SECONDS`: segundos para purgar trenes no vistos.
- `SNAPSHOT_RETENTION_HOURS`: retención de snapshots (`0` = conservar todo; valor por defecto).
- `INGESTION_RUN_RETENTION_DAYS`: retención de ejecuciones de ingesta (`0` = conservar todo).
- `SNAPSHOT_HEARTBEAT_SECONDS`: inserta snapshot aunque no cambie el tren cada N segundos.
- `COMPACT_EVERY_RUNS`: frecuencia de limpieza configurada, `PRAGMA optimize` y checkpoint WAL; no ejecuta `VACUUM`.
- `DISPLAY_CACHE_DIR`: carpeta de cache JSON de dashboard precomputado.
- `DISPLAY_CACHE_24_SECONDS`: refresco de caché 24h (60 segundos por defecto).
- `DISPLAY_CACHE_168_SECONDS`: refresco de caché 7d (60 segundos por defecto).
- `DISPLAY_CACHE_720_SECONDS`: refresco de caché 30d (60 segundos por defecto).
- `API_KEY_TTL_SECONDS`: duración de la clave API temporal.
- `API_RATE_LIMIT_MS`: intervalo mínimo entre peticiones por clave (default `200`).
- `RAW_MAX_TRAINS`: máximo de trenes en `/api/raw/live`.
- `OBSERVATION_RETENTION_DAYS`: ventana de detalle en SQLite (`0` = conservar todo). Solo se retiran filas con archivado verificado y `ARCHIVE_ENABLED=1`.
- `HOURLY_RETENTION_DAYS`: retención de `train_hourly_train_stats` (`0` = conservar todo).
- `DAILY_RETENTION_DAYS`: retención de `train_daily_stats` (`0` = indefinido).
- `BATCH_RETENTION_DAYS`: retención de `ingestion_batches` (`0` = conservar todo y sus relaciones).
- `ARCHIVE_ENABLED`: crea archivos de observaciones antiguas (desactivado por defecto). Los archivos ya existentes siguen siendo consultables.
- `ARCHIVE_DIR`: directorio destino para `JSONL.zst` por hora.
- `ARCHIVE_ZSTD_LEVEL`: nivel de compresión Zstd (1-19).
- El archivado nuevo requiere `ARCHIVE_ENABLED=1` y `OBSERVATION_RETENTION_DAYS>0`, además del binario `zstd`. Las demás tablas conservan sus propias retenciones.
- `WAL_CHECKPOINT_TRUNCATE_BYTES`: tamaño mínimo del WAL para checkpoint `TRUNCATE`.
- `HTTP_COMPRESSION_ENABLED`: activa compresión HTTP (`br`/`gzip`).
- `HTTP_COMPRESSION_MIN_BYTES`: umbral mínimo de tamaño para comprimir respuestas.
- `HTTP_GZIP_LEVEL`: nivel de `gzip` (1-9).
- `HTTP_BROTLI_LEVEL`: nivel de `brotli` (1-11).
- `HISTORY_RETENTION_DAYS`: compat legacy (alias de `OBSERVATION_RETENTION_DAYS`).
- `RECOVERY_LOOKBACK_HOURS`: ventana de recuperación desde snapshots al arrancar.
- `POSTGRES_URL`: conexión para migrar SQLite -> Postgres.
- `SQLITE_PATH`: ruta SQLite para script de migración.
- `MIGRATION_BATCH_SIZE`: lote de inserción en migración (entero entre 1 y 1.000; defecto 500).
- `AUTO_MIGRATE_PG_ON_START`: migración automática a Postgres al arrancar Docker (`1`/`0`, desactivada por defecto). El destino debe estar vacío; el script no sobrescribe datos existentes.
- `BOOTSTRAP_SQLITE_PATH`: ruta de bootstrap de SQLite al primer arranque Docker.
- `BOOTSTRAP_CACHE_PATH`: ruta de bootstrap de cache al primer arranque Docker.

<a id="endpoints-api"></a>
### Endpoints API

Públicos:

- `GET /api/docs`
- `GET /api/health`
- `GET /api/auth/request-key`
- `GET /api/events`: flujo SSE de avisos de ingesta y caché; mantiene la conexión abierta.

Protegidos (requieren `x-api-key`):

- `GET /api/dashboard?historyHours=24|168|720&historyFrom=ISO|epoch&historyTo=ISO|epoch`
- `GET /api/ops?window=24|168|720`
- `GET /api/trains?q=&minDelay=&limit=&offset=`
- `GET /api/trains/:codComercial/history?hours=24`
- `GET /api/ingestion/runs?limit=50`
- `GET /api/raw/live`
- `GET /api/history/coverage?hours=48`
- `POST /api/history/recover?hours=48`

Idioma de API:

- sin parámetro `lang` en URL.
- usa cabecera `Accept-Language` (`es` o `en`).

Precisión numérica:

- los campos acumulados grandes (por ejemplo `accumulatedDelayMinutes`) se exponen como `string` para evitar pérdida de precisión con valores muy altos.
- `GET /api/dashboard` añade `historyPrecision` para indicar si el rango salió de detalle crudo o agregado (`raw_detail`, `aggregated_only`, `mixed`).
- `GET /api/history/coverage` añade `storage` con `dbRows`, `archivedRows`, `archiveChunks`.

<a id="flujo-consumo-api"></a>
### Flujo de consumo API

1. Solicita clave temporal:
   - `GET /api/auth/request-key`
2. Usa la clave en cabecera:
   - `x-api-key: <tu_clave>`
3. Respeta límites:
   - 1 petición en curso por clave.
   - intervalo indicado en `limits.minIntervalMs` al solicitar la clave. No se reinicia la espera al terminar una petición.

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

- `app` en `http://localhost:3011` (puerto 3000 dentro del contenedor)
- `postgres` en `localhost:5432` (`renfe/renfe`, DB `renfe_analytics`)

Persistencia entre sesiones:

- `appdata` para SQLite + cache (`/app/data`)
- `pgdata` para Postgres (`/var/lib/postgresql/data`)

Integración automática al arrancar Docker:

1. Si `appdata` está vacío y existe `./data/renfe.db`, se importa automáticamente.
2. Si existe `./data/cache/flotaLD.latest.json`, también se importa.
3. La migración a PostgreSQL está desactivada por defecto. `AUTO_MIGRATE_PG_ON_START=1` la solicita sobre un destino vacío.

El bootstrap copia SQLite y la caché; usa una instantánea coherente, nunca el archivo de una base con escrituras activas. Para trasladar el histórico comprimido, copia también sus archivos a `appdata` y conserva las rutas que figuran en `observation_archive_chunks`. No borres el volumen existente para volver a importar datos.

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

El script exige tablas destino vacías y copia las 12 tablas de aplicación, incluidas las tablas Ops, dentro de una transacción PostgreSQL. Lee SQLite por lotes sobre una instantánea estable y comprueba recuentos. Si encuentra datos en el destino, aborta sin sobrescribirlos. Los archivos JSONL.zst se trasladan por separado; copiar el manifiesto no sustituye sus archivos.

### Licencia

Este proyecto se distribuye bajo:

- `Creative Commons Attribution-NonCommercial 4.0 International (CC BY-NC 4.0)`

---

Este proyecto consume endpoints públicos desde frontend oficial de RENFE y está pensado para analítica/monitorización técnica.

<a id="conservacion-actualizacion"></a>
### Conservación y actualización

Todas las retenciones están desactivadas por defecto. La ingesta se confirma en una única transacción SQLite con `synchronous=FULL`. Los archivos históricos se comprueban contra su SHA-256 al consultarlos y el archivado nuevo conserva todos los campos, incluido `id`.

El servidor entrega los datos actuales y los trenes en una petición. Los cálculos históricos se ejecutan en un Worker de Bun, con cachés de 60 segundos para 24h, 7d y 30d. `GET /api/events` avisa de cada ingesta confirmada y de las cachés actualizadas; las páginas se actualizan automáticamente. Si falta una caché, el dashboard muestra los datos actuales mientras se prepara el histórico. La fuente se consulta cada `POLL_INTERVAL_MS` (60 segundos por defecto).

`GET /api/dashboard` incluye `mapTrains` con todos los trenes que coinciden con `q` y `minDelay`, independientemente de `limit` y `offset`, y `mapAnalytics` con velocidades por servicio de las últimas 24 h y tramos con retrasos tanto recientes como del histórico completo. El mapa conserva el zoom y la posición al actualizarse; «Ver la red» encuadra los trenes y las zonas visibles. Descarta coordenadas nulas, fuera del rango del mapa o `(0, 0)`. Si falla la ingesta o se usa la caché local, identifica las posiciones como últimas conocidas. No interpola movimiento entre señales GPS.

El mapa se amplía con `Ctrl` + rueda; sin `Ctrl`, la rueda desplaza la página. En móvil admite el gesto de dos dedos y conserva botones compactos de zoom. Puedes seleccionar un tren desde su marcador, el selector del mapa o su número en la tabla, incluso cuando varios comparten posición. La información aparece en una tarjeta superpuesta al mapa, con cierre mediante su botón o `Esc`, desplazamiento interno y adaptación a móvil. Muestra tipo, retraso, trayecto, próxima estación, corredor, coordenadas y horas de GPS e ingesta. «Centrar tren» acerca la última posición publicada a zoom 16 y la deja visible junto a la tarjeta; el enlace a OpenStreetMap permite abrir ese punto por separado. La selección y la ficha se actualizan con la ingesta sin cambiar el zoom, y se limpian si el tren desaparece o queda fuera de los filtros.

El análisis usa las horas reales del GPS para estimar distancia entre señales / tiempo. La media pondera la duración de los intervalos válidos e incluye paradas; puede subestimar el recorrido en curvas. Se excluyen señales repetidas, observaciones recuperadas o estimadas, coordenadas inválidas, GPS alejado más de 180 s de la captura, huecos de más de 180 s y velocidades superiores a 400 km/h. La velocidad del último intervalo se oculta si lleva más de 180 s sin una señal válida. Los servicios se separan por tren, producto, origen, destino y día operativo de Madrid, conservando el criterio existente de corte a las 04:00.

Las zonas negras se detectan en celdas aproximadas de 0,025 grados (unos 2–3 km) donde al menos 3 intervalos y 2 servicios acumulan retraso positivo. Deben representar al menos el 20 % de los intervalos válidos de la celda. Frenar sin ganar retraso o perder margen sin llegar tarde no crea una zona negra. Los aumentos de retraso mayores que la duración del intervalo más un minuto se descartan como correcciones no atribuibles al intervalo. Se resaltan únicamente las geometrías ferroviarias reales de OpenStreetMap recortadas a esas celdas; se omiten las zonas sin vías de referencia. La detección no atribuye el retraso a una vía paralela concreta ni identifica su causa. Las zonas son de toda la red y no dependen de los filtros de trenes ni del selector de estadísticas históricas.

El selector del mapa alterna entre «Histórico completo», ordenado por minutos de retraso añadidos, y «Recientes · 24 h», ordenado por el último aumento registrado. Ambos muestran cinco tramos destacados que se pueden pulsar para centrar el mapa y abrir su ficha. Las fichas indican servicios afectados, retraso añadido total y por servicio afectado, mayor retraso observado y fecha del último aumento. La estación cercana sirve como referencia geográfica, sin atribuirle la causa del retraso.

`mapAnalytics` versión 3 se calcula en el Worker y se guarda en la caché de 24 h, con refresco cada 60 s por defecto. `zones` contiene los tramos recientes y `history.zones` los del histórico completo, ambos con `railSegments` en coordenadas `[latitud, longitud]`. `history` incluye cobertura y progreso. La tabla derivada `map_delay_days` conserva resúmenes diarios con los identificadores de los servicios afectados para no duplicarlos al cruzar medianoche. Se leen también los archivos históricos verificados. La primera carga completa siete días cerrados por tanda y continúa en segundo plano, mostrando el progreso; los días cerrados se reutilizan y las inserciones tardías invalidan los resúmenes afectados. El día actual se recalcula. Las observaciones originales se conservan. La API no recorre todo el histórico ni recorta geometría en cada petición. La media de toda la red sigue retirada y las velocidades de cada tren se conservan.

La geometría de referencia está en `src/data/railways.geojson`, con licencia ODbL y atribución separadas de la licencia del código. `python3 scripts/update-railways.py` la renueva desde OpenStreetMap/Overpass; el Worker la carga una sola vez por proceso. La extracción, cobertura y actualización se describen en [src/data/README.md](src/data/README.md). La web explica el significado de las vías negras, sin mostrar fórmulas ni instrucciones de gestos.

El mapa usa Leaflet 1.9.4, MapLibre GL JS 5.6.2 y su adaptador Leaflet 0.1.3, servidos desde `public/vendor/` con sus licencias. La cartografía vectorial usa los estilos claro Positron y Dark de [OpenFreeMap](https://openfreemap.org/quick_start/), sin clave API, y cambia con el tema. Mantiene las atribuciones de OpenFreeMap, OpenMapTiles y OpenStreetMap. Los dispositivos sin WebGL2 conservan la cartografía raster de OpenStreetMap y su [política de teselas](https://operations.osmfoundation.org/policies/tiles/). La cartografía requiere conexión; si falla, se muestra un aviso y los datos de trenes siguen disponibles.

Verificación reproducible:

```bash
bun run check
bun run test
node --check public/app.js
node --check public/ops-lab.js
git diff --check
```

Las regresiones usan una base temporal y prueban retención cero, recuperación idempotente, rollback de ingesta, fidelidad y corrupción de archivos, detalle archivado, bordes horarios, caché fría, autenticación, concurrencia HTTP, SSE y aislamiento del Worker. El mensaje `[ingestor] error test failure` corresponde al fallo provocado para comprobar rollback; el resultado final debe ser `OK` y el proceso terminar con código 0.

Mantenimiento local: `python3 scripts/compact-storage.py` detiene temporalmente `retrasometro.service`, respalda SQLite, retira el índice redundante y ejecuta `VACUUM`. Reanuda el servicio antes de comparar todos los campos entre las instantáneas. Conserva un `backup.tar.zst` con SQLite, archivos históricos y configuración, comprueba su restauración y guarda `verification.json` en `data/backups/<fecha UTC>/`. No borra las copias anteriores. El script apunta a `data/renfe.db` de este checkout y al servicio `retrasometro.service`; no toma `DB_PATH` del entorno ni opera sobre Docker. Requiere Python 3.11+, Zstd, acceso a systemd y espacio para varias instantáneas temporales. La compactación puede tardar varios minutos en una base grande.

La migración a PostgreSQL lee por lotes dentro de una instantánea estable y incluye las tablas Ops. Los archivos JSONL.zst deben acompañar al manifiesto: el script SQL no los traslada.

<a id="operacion-servicio"></a>
### Operación del servicio y restauración

En el servidor aplicado, el checkout es `/root/retrasometro`, Bun escucha en `127.0.0.1:3011` y Nginx sirve `https://retrasometro.com`. El servicio usa `bun run start`; los cambios de TypeScript requieren reiniciarlo. La configuración local y el drop-in de systemd se mantienen fuera de Git.

```bash
cd /root/retrasometro
bun run check
bun run test
systemctl restart retrasometro.service
systemctl is-active retrasometro.service
curl --fail --silent http://127.0.0.1:3011/api/health
tail -n 30 /var/log/retrasometro.log
```

El drop-in `/etc/systemd/system/retrasometro.service.d/runtime-hotfix.conf` conserva estos valores. Reprodúcelos si trasladas este servicio; los valores de systemd prevalecen sobre `.env`:

```ini
[Service]
Environment=ARCHIVE_ENABLED=0
Environment=OBSERVATION_RETENTION_DAYS=0
Environment=COMPACT_EVERY_RUNS=1440
Environment=SNAPSHOT_RETENTION_HOURS=0
Environment=INGESTION_RUN_RETENTION_DAYS=0
Environment=HOURLY_RETENTION_DAYS=0
Environment=DAILY_RETENTION_DAYS=0
Environment=BATCH_RETENTION_DAYS=0
Environment=DISPLAY_CACHE_168_SECONDS=60
Environment=DISPLAY_CACHE_720_SECONDS=60
```

Después de cambiar un drop-in, ejecuta `systemctl daemon-reload` y reinicia el servicio. Para SSE, el proxy debe permitir una conexión larga y transmitir los eventos sin acumularlos; la respuesta incluye `X-Accel-Buffering: no` y un comentario de mantenimiento cada 20 segundos. El servidor desactiva el timeout únicamente para esa ruta. HTML, JavaScript y CSS se sirven con `no-cache`, y la web debe recargarse una vez después del despliegue.

El backup verificado del 7 de octubre se creó a las **16:39:42, hora de Madrid**. Su carpeta usa UTC: `data/backups/20261007T143942Z/`. Contiene `backup.tar.zst` y `verification.json`. La [verificación conservada en Git](docs/storage-verification-2026-10-07.json) permite comprobar una copia del paquete sin incluir los datos ni la configuración privada en el repositorio.

Para comprobar la restauración en un directorio aislado:

```bash
umask 077
backup=data/backups/20261007T143942Z/backup.tar.zst
restore_dir=$(mktemp -d /tmp/retrasometro-restore.XXXXXX)
zstd -q -t "$backup"
python3 - "$backup" docs/storage-verification-2026-10-07.json <<'PYHASH'
import hashlib, json, sys
from pathlib import Path
report = json.loads(Path(sys.argv[2]).read_text())
with Path(sys.argv[1]).open('rb') as stream:
    assert hashlib.file_digest(stream, 'sha256').hexdigest() == report['backup_sha256']
print('OK: SHA-256 del paquete completo')
PYHASH
zstd -q -d -c "$backup" | tar -x -C "$restore_dir"
python3 - "$restore_dir/data/renfe.db" docs/storage-verification-2026-10-07.json <<'PYVERIFY'
import hashlib, json, sqlite3, sys
from pathlib import Path
path = Path(sys.argv[1]).resolve()
report = json.loads(Path(sys.argv[2]).read_text())
with path.open('rb') as stream:
    assert hashlib.file_digest(stream, 'sha256').hexdigest() == report['sqlite_backup_sha256']
db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
try:
    assert db.execute('PRAGMA quick_check').fetchall() == [('ok',)]
    for name, expected in report['counts_before'].items():
        assert db.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0] == expected
finally:
    db.close()
print('OK: SHA-256, integridad y recuentos de SQLite restaurada')
PYVERIFY
```

Usa el `verification.json` de la misma carpeta cuando compruebes otro backup. La extracción anterior deja `.env` y los archivos históricos dentro del directorio aislado y conserva la base activa. Para poner una restauración en servicio, detén primero la ingesta y guarda la base actual junto con su WAL, SHM, archivos históricos y configuración. Sustituirla por un backup anterior retrocede al instante de esa captura; las escrituras posteriores deben conservarse o recuperarse por separado. Al trasladar el proyecto, verifica que las rutas del manifiesto resuelvan a los archivos restaurados.

Los resultados, la pausa de mantenimiento, los límites de frescura y las comprobaciones de conservación están en [STORAGE_REVIEW.md](STORAGE_REVIEW.md). La compactación ya se aplicó; no hace falta ejecutarla de nuevo al instalar este commit.
