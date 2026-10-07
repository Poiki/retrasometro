**Aplicación completada.** La sección final recoge los cambios desplegados, las mediciones y la verificación completa. El análisis inicial siguiente describe el estado anterior a esas correcciones.

**Revisión de almacenamiento y código — 7 de octubre de 2026**

Criterio: Ponytail ultra y ai-slop-cleaner; conservar toda la información existente. Revisé código, configuración, base local y archivos históricos. Las conexiones a la base original fueron de solo lectura. Las pruebas de compactación y retirada de índices se hicieron en una copia temporal, ya retirada. No cambié la aplicación, la configuración ni los datos originales.

**Resultado: la base puede pasar de 5,82 a 4,38 GiB, un 24,8 % menos, sin eliminar registros.** Es una medición sobre una copia consistente de los datos reales. El archivado comprimido permite ahorrar más, pero su implementación actual necesita correcciones para conservarlo todo.

**Qué ocupa espacio**

La aplicación activa usa Bun y SQLite en `data/renfe.db`. PostgreSQL es el destino de un script de migración; el servidor actual no lo usa como motor.

| Elemento | Tamaño aproximado | Observación |
| --- | ---: | --- |
| SQLite principal | 5,82 GiB | 6.246.133.760 bytes al comenzar la prueba |
| Tabla `train_observations` | 3,17 GiB | Unas 24,88 millones de filas |
| Sus tres índices | 2,18 GiB | Tabla e índices suman el 92 % de la base |
| Índice `idx_observations_train_time` | 897 MiB | Candidato redundante, comprobado sobre copia |
| Histórico Zstd | 99,18 MiB | 508 archivos, 3.022.040 observaciones |
| Copias antiguas en `data/backups` | 1,46 GiB | Incluyen una base y su WAL; se conservaron |
| Caché | Unos 136 KiB | Limpiarla no aporta ahorro relevante |
| WAL activo | Unos 24–27 MiB durante la revisión | Variable; no explica los GiB de la base |

Las observaciones en SQLite comienzan el 1 de abril de 2026 a las 23:00, hora de Madrid. Los archivos anteriores abarcan desde el 11 de marzo hasta el 1 de abril. Las cifras varían ligeramente porque la ingesta continúa.

El proceso activo tiene `OBSERVATION_RETENTION_DAYS=0` y `ARCHIVE_ENABLED=0`. Aunque `.env` contiene `ARCHIVE_ENABLED=1`, el entorno del proceso sobrescribe ese valor. Además, la limpieza solo llama al archivador cuando la retención de observaciones es mayor que cero: cambiar únicamente la bandera no basta.

**Opciones, por orden de utilidad**

1. **Compactar con SQLite.** `VACUUM INTO` produjo una copia de 5.196.288.000 bytes: **4,84 GiB**, un ahorro de unos **0,98 GiB**. Aunque `freelist_count` era cero, había huecos dentro de páginas parcialmente ocupadas. La copia pasó `PRAGMA quick_check`. [SQLite documenta que esta operación conserva el contenido lógico y genera una instantánea consistente](https://www.sqlite.org/lang_vacuum.html). Una copia hecha mientras continúa la ingesta no puede sustituir luego a la base activa sin incorporar las escrituras posteriores. Aplicarlo requiere controlar ese intervalo, conservar un backup recuperable y reservar espacio temporal.

2. **Retirar un índice redundante y compactar.** [src/db.ts:338](src/db.ts#L338) crea `(cod_comercial, captured_at DESC)` además del índice único `(cod_comercial, captured_at, source)`. Las consultas actuales por tren pueden usar el índice único en la dirección necesaria. No encontré dependencias `INDEXED BY` del candidato. `EXPLAIN QUERY PLAN` confirmó que la consulta de historial ya usa el índice único, también después de retirar el otro en una base de prueba. [El planificador de SQLite explica el aprovechamiento de las primeras columnas de un índice compuesto](https://www.sqlite.org/queryplanner.html).

   En la copia completa, retirar ese índice y compactar dejó **4.699.570.176 bytes: 4,38 GiB**. Se mantuvieron los recuentos de las 12 tablas de aplicación. Ahorro combinado: **1,44 GiB, un 24,8 %**. El ahorro adicional del índice sobre la copia ya compactada fue de unos 474 MiB; no se deben sumar sus 897 MiB originales al ahorro de compactación. Antes de aplicarlo, retirar también su creación del esquema y comprobar tiempos de las consultas afectadas. Mantener el índice único y el índice global por fecha.

3. **Comprimir el histórico completo, conservando detalle reciente en SQLite.** Reutilizar `JSONL.zst` con Zstd nivel 3. Hay unas 1,65 millones de observaciones de los últimos 14 días y 3,80 millones de los últimos 30 días. Se puede conservar una de esas ventanas en SQLite y trasladar el resto a archivos, incluyendo cada fila y sus metadatos.

| Datos medidos | JSON sin comprimir | Zstd nivel 3 | Tamaño restante |
| --- | ---: | ---: | ---: |
| Los 508 archivos existentes | 1.299.960.029 bytes | 103.998.800 bytes | 8,00 % |
| 10.000 observaciones antiguas, incluyendo `id` | 4.279.017 bytes | 359.167 bytes | 8,39 % |
| 10.000 observaciones recientes, incluyendo `id` | 4.309.078 bytes | 395.208 bytes | 9,17 % |

En las muestras, descomprimir devolvió exactamente los bytes originales. Los porcentajes se refieren al JSON, no al archivo SQLite. [Zstandard ofrece compresión sin pérdida](https://github.com/facebook/zstd). El nivel 9 redujo las muestras un 10–11 % adicional respecto al nivel 3, con mayor coste de compresión; empezar por nivel 3 basta.

Una ventana activa de 14 días más todo el histórico comprimido podría situar SQLite y archivos alrededor de **1,4–1,8 GiB**, excluyendo backups. Es una estimación basada en filas recientes y muestras, no una medición de ese traslado completo. El detalle antiguo sería más lento y requiere lectura de archivos o restauración selectiva. Después de retirar filas ya archivadas, SQLite necesitaría compactación o reutilizaría el espacio liberado.

4. **Comprimir backups coherentes.** Crear la instantánea mediante la [API de backup de SQLite](https://www.sqlite.org/backup.html), comprimirla con Zstd y comprobar una restauración independiente. Incluir también archivos históricos y manifiesto. La app necesita una base SQLite descomprimida para consultarla. No se midió la compresión de los backups completos. No borrar ni copiar aisladamente el WAL de una base activa: [puede contener transacciones confirmadas](https://www.sqlite.org/wal.html). Las copias antiguas requieren comprobar su restauración antes de cualquier sustitución o eliminación.

**Problemas que impiden prometer conservación completa**

- **El archivador borra confiando solo en el manifiesto.** En [src/db.ts:2284](src/db.ts#L2284), si hay una entrada para el archivo, elimina observaciones de su rango sin comprobar que el archivo exista, que su SHA-256 coincida o que contenga esas mismas filas. El control de cantidades ocurre después del borrado y esa rama no tiene transacción. Reproducción en memoria: una fila ausente del supuesto archivo se borró sin que el control lanzara error. Corrección mínima: conservar las filas locales ante archivos ya registrados hasta verificar contenido; borrar únicamente filas cuya equivalencia completa esté comprobada. Mantener las garantías existentes de escritura temporal, `fsync`, renombrado y transacción de los archivos nuevos.

- **Se purga información aunque las observaciones tengan retención indefinida.** [src/ingestor.ts:322](src/ingestor.ts#L322) elimina snapshots de más de 96 horas, ejecuciones de más de 30 días y lotes de más de 14 días; la retención horaria es de 400 días. Al borrar lotes, `ON DELETE SET NULL` en [src/db.ts:336](src/db.ts#L336) destruye el vínculo con la ingesta original. Las 10.000 observaciones antiguas muestreadas ya tienen `batch_id=NULL`. Conservar o archivar esas tablas y desactivar las purgas configurables mientras se prepara el traslado. Snapshots y ejecuciones necesitan un cambio de código para permitir retención indefinida: `SNAPSHOT_RETENTION_HOURS=0` ahora adelanta el corte al presente, no desactiva la purga. Los metadatos ya purgados no se reconstruyen desde estas observaciones; los backups podrían recuperar una parte, pendiente de comprobación.

- **Los archivos omiten la clave primaria `id`.** La selección de [src/db.ts:2304](src/db.ts#L2304) exporta 20 campos y omite ese identificador; lo confirmé en archivos existentes. Incluirlo en futuros archivos y preservar relaciones y secuencias al restaurar. Conservar valores exactos, nulos y procedencia: no redondear coordenadas ni sustituir detalle por medias.

- **La API no lee en general el detalle archivado.** El lector de [src/db.ts:1034](src/db.ts#L1034) alimenta métricas Ops. El historial por tren consulta SQLite y los rangos antiguos del dashboard usan agregados. Incluso [la consulta horaria](src/db.ts#L3334) necesita observaciones crudas para las horas de los extremos: al archivarlas puede omitir esa parte. Añadir restauración selectiva o lectura de archivos antes de trasladar datos. Conservar un archivo no garantiza que la API siga mostrando su contenido.

- **La recuperación añade filas aunque ya existan observaciones reales.** [src/db.ts:2860](src/db.ts#L2860) inserta `source='recovered_snapshot'`; como la unicidad incluye `source`, `INSERT OR IGNORE` no detecta la observación real del mismo tren e instante. Se llama al arrancar y desde el endpoint de recuperación. Ejecuté su SQL original en memoria: una observación real pasó a coexistir con una estimada. La ventana actual de 72 horas tenía **414.297 snapshots** candidatos a duplicarse en el siguiente arranque. Corregirlo en la función compartida comprobando existencia por tren e instante. Conservar las filas actuales hasta decidir cómo mantener su trazabilidad.

- **Durabilidad mejorable.** [src/db.ts:214](src/db.ts#L214) usa `synchronous=NORMAL`: con WAL, una transacción reciente puede revertirse tras un corte eléctrico o fallo del sistema. Para priorizar conservación, valorar `FULL`, medir el coste y agrupar la ingesta en una transacción coherente. [SQLite documenta la diferencia](https://www.sqlite.org/pragma.html#pragma_synchronous). Esto complementa los backups; no reduce el tamaño.

**Revisión ai-slop-cleaner**

La simplificación de mayor efecto es retirar el índice redundante y evitar la recuperación duplicada. Oportunidades de menor prioridad:

| Localización | Hallazgo | Cambio mínimo propuesto |
| --- | --- | --- |
| `src/db.ts:3334` y `src/db.ts:3635` | SQL y montaje de respuesta repetidos en dos caminos históricos | Compartir procesamiento común manteniendo fuentes y límites; verificar bordes antes |
| `src/dashboard-cache.ts` y `src/ops-cache.ts` | Intervalos y escritura JSON temporal duplicados | Reutilizar funciones pequeñas, sin jerarquía genérica de cachés |
| `src/index.ts:20` | Cada cambio recalcula las tres ventanas Ops y evita sus intervalos de refresco | Usar el refresco programado si su frescura resulta suficiente |
| `src/auth.ts:9` | `lastCompletedAt` y `totalRequests` se escriben pero no se leen | Retirar esos campos si no hay consumidor externo; conservar expiración y límites |
| `src/ops-cache.ts:10` | Interfaz vacía sobre el tipo de respuesta | Usar el tipo existente |
| `src/db.ts:473` | Migraciones que silencian cualquier error | Comprobar columnas existentes y propagar fallos reales |
| `scripts/migrate-sqlite-to-postgres.ts:292` | Carga tablas completas en memoria antes de dividirlas en lotes | Leer por lotes sobre una instantánea estable |

La migración PostgreSQL tampoco constituye un backup completo del proyecto actual: [vacía tablas destino](scripts/migrate-sqlite-to-postgres.ts#L205), no copia `ops_service_hourly` ni `ops_materialization_state` y copia rutas del manifiesto sin trasladar sus archivos. Docker tiene esa migración activada al arrancar. Resolver esas diferencias antes de utilizarla para conservación. No hace falta cambiar de motor para obtener los ahorros medidos.

**Orden propuesto y verificación**

Plan de aplicación solicitado: añadir regresiones de recuperación, archivado y retención; corregir esas rutas sin borrar históricos; aislar ingesta y cálculos pesados del servidor HTTP; servir datos vivos sin recalcular el histórico y actualizar la web automáticamente; respaldar, compactar y verificar la base; comprobar tiempos y frescura en el servicio real. La migración y las simplificaciones menores se revisarán después de proteger estos comportamientos.

1. Proteger toda la información existente: corregir purgas incompatibles y evitar nuevas recuperaciones duplicadas.
2. Crear un backup coherente de SQLite, archivos y manifiesto; comprobar restauración independiente.
3. Compactar y retirar el índice candidato de forma controlada; evitar perder escrituras posteriores a una instantánea y verificar datos y consultas.
4. Si hace falta más ahorro, corregir el archivador y el acceso a detalle antiguo. Probar una hora completa, restaurarla y comparar todos los campos antes de retirar filas de SQLite.
5. Mantener backups independientes. Agregados diarios y horarios ayudan a consultar, pero no sustituyen observaciones completas.

Comprobaciones ejecutadas: TypeScript (`./node_modules/.bin/tsc --noEmit`) sin errores; SHA-256 de los 508 archivos contra el manifiesto; `zstd -t` de todos; lectura de sus 3.022.040 filas con recuentos y extremos temporales correctos; compresión y descompresión byte a byte de dos muestras con `id`; planes de consulta; reproducción en memoria de los dos fallos; `quick_check` de la copia compactada antes de retirar el índice; recuentos de las 12 tablas iguales después de retirarlo y compactar otra vez.

En la revisión inicial no se ejecutó `integrity_check` completo ni una restauración integral de los archivos a SQLite. Recuentos y `quick_check` no equivalen a comparar todos los campos de todas las filas. En aquel momento no había comprobaciones de regresión del archivado y la recuperación en el repositorio: las correcciones deben dejar comprobaciones pequeñas y ejecutables de esos casos. El único cambio de aquella revisión inicial fue este informe; los cambios posteriores se describen a continuación.

**Cambios aplicados y desplegados**

- SQLite compactada en el archivo activo y retirado `idx_observations_train_time`. De **6.249.504.768 a 4.702.347.264 bytes**: **5,82 → 4,38 GiB**, ahorro de **1,44 GiB (24,76 %)**. Se conservan el índice único y el índice global por fecha. Un índice parcial por fecha acelera la detección de corredores oficiales y evita recorrer todo el histórico.
- Desactivadas las purgas de observaciones, snapshots, lotes, ejecuciones y agregados, tanto en los valores por defecto como en `.env`, Docker y el servicio activo. La actualización de `trains_current` sigue representando los trenes activos; cada captura se conserva en el histórico.
- Ingesta atómica con `synchronous=FULL`; la recuperación comprueba existencia por tren e instante para evitar duplicados. Se conservaron todas las observaciones existentes, incluidas las estimadas antiguas.
- Archivado corregido: exporta `id`, verifica la descompresión byte a byte antes de retirar filas, exige equivalencia completa al borrarlas y conserva residuos de un archivo ya registrado. El detalle archivado se incorpora a las consultas en una tabla temporal, con verificación de hash y manifiesto. **El traslado masivo a archivos permanece desactivado**; el detalle existente en SQLite sigue disponible con la misma rapidez.
- Los cálculos históricos, recuperaciones, cobertura y métricas Ops se ejecutan en un Worker nativo de Bun. El dashboard usa la caché histórica y entrega los trenes actuales en la misma petición. Sin caché, muestra el dato actual mientras se prepara el histórico. Las ventanas 24h/7d/30d se refrescan cada 60 segundos.
- La web principal y Ops Lab reciben avisos SSE después de cada ingesta o actualización de caché. Se retiró la espera por inactividad del usuario. Hay reconexión automática y sondeo de respaldo; se conserva la expiración, el límite temporal y la exclusión de peticiones simultáneas por clave.
- Limpieza: procesamiento histórico común compartido, escritura JSON e intervalos reutilizados, campos y métodos muertos retirados, migraciones SQLite que propagan errores reales. Sin dependencias nuevas. La migración PostgreSQL lee mediante iterador por lotes sobre una instantánea estable, incluye las tablas Ops y rechaza destinos con datos; Docker no la ejecuta automáticamente.

**Prueba de conservación**

La operación detuvo el servicio durante **252,89 segundos (4 min 13 s)**. Se respaldó la base detenida antes de modificarla y se tomó otra instantánea después de compactarla. El servicio se reanudó antes de las comprobaciones largas; las escrituras posteriores siguen en la base activa y no se sustituyó por una copia antigua.

Se comparó la serialización de **cada campo de cada fila** entre ambas instantáneas, ordenando por clave primaria: las 12 tablas coinciden, incluidas **24.884.752 observaciones**, 580.427 snapshots y las secuencias SQLite. Ambas pasan `PRAGMA quick_check`. Los **508 archivos históricos** conservan sus SHA-256. Los recuentos y hashes por tabla están en [verificación conservada en Git](docs/storage-verification-2026-10-07.json).

Backup completo: [backup.tar.zst](data/backups/20261007T143942Z/backup.tar.zst), **1.754.798.290 bytes (1,63 GiB)**. Incluye SQLite, todos los archivos del manifiesto y `.env`. Se descomprimió a una base independiente: SHA-256 idéntico al backup SQLite original, `quick_check` correcto y recuentos idénticos; los archivos restaurados también coinciden por hash. Se retiraron únicamente las instantáneas temporales de esta operación después de verificar la restauración. Las copias anteriores permanecen intactas. El nuevo backup ocupa espacio adicional; el 24,76 % de ahorro se refiere a la base activa.

**Rendimiento y comprobaciones**

| Ruta | Antes | Después |
| --- | ---: | ---: |
| Dashboard | 4.528 ms | Mediana 9,12 ms; máximo 11,80 ms |
| Ops 7d | 73 ms | Mediana 1,09 ms; máximo 1,49 ms |
| Trenes | 4 ms | Mediana 3,71 ms; máximo 4,22 ms |

Mediciones HTTP locales; 12 peticiones por ruta en la medición final con compresión gzip. En Chromium sobre la web pública se mostraron datos en aproximadamente **247 ms**, se observó una actualización automática tras la siguiente ingesta mientras había interacción, y se probaron idioma y ventanas Ops sin errores JavaScript ni respuestas HTTP fallidas. No representan la latencia de todas las conexiones de usuarios.

Pasaron `bun run check`, `bun scripts/check-regressions.ts`, la comprobación sintáctica de ambos scripts web y `git diff --check`. La comprobación ejecutable cubre recuperación idempotente, retención cero, rollback de ingesta, fidelidad y corrupción de archivos, consultas archivadas, bordes horarios, caché fría, límites de autenticación, API concurrente y avisos SSE sostenidos; comprueba también que el Worker utilice la base temporal indicada.

Los archivos afectados están en `src/{db,ingestor,index,server,analytics-worker,config,auth,i18n,dashboard-cache,ops-cache,utils}.ts`, `public/{app.js,ops-lab.js,index.html,ops-lab.html}`, `scripts/{check-regressions.ts,compact-storage.py,migrate-sqlite-to-postgres.ts}`, configuración y README. El mantenimiento y la verificación se pueden repetir con `python3 scripts/compact-storage.py`.

Límites: la fuente continúa consultándose cada 60 segundos; la notificación muestra la captura confirmada inmediatamente, pero no adelanta datos que Renfe aún no haya publicado. Durante la pausa de compactación no hubo ingesta; la fuente no permite reconstruir retrospectivamente esos ciclos. Los metadatos ya purgados y los `id` omitidos en archivos antiguos no se regeneraron. Las consultas amplias a detalle archivado requieren descompresión y pueden tardar más; se aíslan del servidor HTTP. PostgreSQL se comprobó por tipos y revisión del flujo, sin ejecutar una migración contra un destino real.

La configuración efectiva del servicio, las instrucciones de despliegue y la comprobación de restauración aislada están documentadas en [README.md](README.md#operacion-servicio). El backup y `.env` permanecen en almacenamiento privado; Git conserva código, documentación y metadatos de verificación.
