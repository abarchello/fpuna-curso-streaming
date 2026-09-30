# Trabajo práctico final: telemetría hidrometeorológica en streaming

Kafka, Apache Beam, Flink y Marimo. *Streaming de datos y sus aplicaciones*,
MIAAD FP-UNA. Autor: `abarchello`.

Este proyecto toma lecturas históricas de estaciones meteorológicas
automáticas de Alto Paraná y Canindeyú y las reproduce como si llegaran en
vivo. Un pipeline de Apache Beam corriendo sobre Flink las procesa por tiempo
de evento y calcula la lluvia acumulada por estación cada 10 minutos y por
subcuenca cada hora, además de alertas de lluvia intensa y de ráfagas. Es la
continuación de las Tareas 1 a 3: el mismo dominio, las mismas reglas de
tiempo y la misma clave idempotente.

![Arquitectura](img/arquitectura.png)

## 1. Motivación

Con El Niño en el pronóstico, que en la región suele traer más lluvia y
crecidas del Paraná, saber cuánto llovió en los últimos 10 minutos o en la
última hora en cada subcuenca es información que se usa para operar. Las
estaciones mandan un dato cada 10 minutos, pero en la práctica los datos no
llegan limpios. Los enlaces GPRS juntan lecturas y las mandan en ráfaga, así
que llegan desordenadas. Las estaciones satelitales se atrasan entre 10 y 35
minutos. Los dataloggers reintentan cuando no reciben confirmación y generan
duplicados. Y de vez en cuando aparece un valor imposible.

Un proceso por lotes que corra de noche resuelve todo eso sin esfuerzo, pero
el resultado llega tarde para tomar decisiones. Este pipeline da una
estimación en segundos, un resultado firme al cerrar cada ventana y una
corrección cada vez que llega un dato tardío, sin contar nada dos veces.

Lo pienso para quien vigila la cuenca durante un evento de lluvia: necesita
un tablero con la lluvia reciente por estación y por subcuenca, saber cuántas
estaciones reportaron, y una alerta cuando se llega a un umbral (10 mm o más en 10
minutos, o ráfagas de 20 m/s o más, en esta versión).

## 2. Datos y replay

`dataset.py` genera `data/processed/emas_10min.parquet`: lecturas cada 10
minutos con hasta 8 variables, de las cuales sólo la precipitación es
obligatoria. Hay cuatro fuentes posibles y todas producen el mismo esquema:

- **`openmeteo`** (por defecto). Reanálisis ERA5 bajado de la API pública de
  Open-Meteo (no necesita clave) para 12 localidades del Alto Paraná. Los
  datos son horarios y los paso a 10 minutos respetando el total de lluvia de
  cada hora. Período por defecto: 28 de octubre al 4 de noviembre de 2023, que
  fueron días de lluvias fuertes con El Niño. Es la ejecución normal con
  conexión a internet y lo que usa la demo con Docker.
- **`sintetico`**. Generador que siempre produce los mismos datos: frentes de
  lluvia que cruzan la red hacia el noreste, ciclo de temperatura diario y
  ráfagas cuando llueve. Sirve sin internet, para las pruebas y las demos
  rápidas. Si la descarga falla, `dataset-init` usa esta fuente.
- **`csv`**. Adaptador genérico para un export con una columna por variable y
  un mapeo de columnas en JSON. Sólo la precipitación es obligatoria, y con
  `--start` y `--end` se recorta el período. Es el que uso con los datos
  reales de la sección 8, que no se publican.
- **`marr`**. Adaptador para un export en formato largo: una fila por
  estación, instante y variable, separador `;`, coma decimal y fechas en hora
  local.

A cada estación le asigno un tipo de enlace (`fibra`, `gprs` o `satelital`).
El productor (`producer.py`) publica las lecturas en `ema.lecturas.v1` con
clave `station_id` y con el `event_time` como timestamp de Kafka. Acelera el
tiempo (60 veces por defecto), mete un 2 % de duplicados y atrasa cada
lectura según su tipo de enlace, de modo que el orden de publicación ya no
coincide con el de medición. Así aparecen el desorden y los datos tardíos que
el pipeline tiene que manejar.

## 3. Contratos

El evento de entrada es `ema.lectura`, en JSON, con clave Kafka `station_id`:

```json
{"schema_version": 1, "event_id": "ema-ITA01-20231028T141000Z", "event_type": "ema.lectura",
 "event_time": "2023-10-28T14:10:00Z", "station_id": "ITA01", "station_name": "Hernandarias",
 "subcuenca": "margen-derecha-sur", "lat": -25.4, "lon": -54.62, "seq": 86,
 "precip_mm": 3.2, "temp_c": 27.4, "hr_pct": 81, "presion_hpa": 1004.6,
 "viento_ms": 4.8, "viento_dir_deg": 135, "rafaga_ms": 9.1, "radiacion_wm2": 512,
 "source": "openmeteo-era5", "flag_qc": "OK"}
```

El `event_id` se arma con la estación y la hora de medición
(`ema-<estación>-<event_time>`). Si la misma lectura se reenvía, el id se
repite y se puede deduplicar sin consultar al productor. `decode_event`
rechaza los mensajes a los que les faltan campos, los que tienen timestamps
sin zona horaria y los que traen valores físicamente imposibles; todos esos
van a `ema.lecturas.dlq.v1`.

La salida va a `ema.agregados.v1`, con clave Kafka `aggregate_id`. Es un
tópico compactado: por cada `aggregate_id` Kafka termina guardando sólo el
último pane, así que funciona como una tabla que se puede releer entera.

| Campo | Contenido |
|---|---|
| `aggregate_id` | `metric_type|window_start|dimension_id`, la clave idempotente |
| `metric_type` | `estacion_10min`, `subcuenca_1h` o `alerta` |
| `window_start`, `window_end` | ventana en tiempo de evento (ISO, UTC) |
| `pane_index`, `pane_timing`, `is_first`, `is_last` | qué versión del resultado es (EARLY, ON_TIME o LATE) |
| Métricas de `estacion_10min` | `precip_mm`, `temp_media/min/max_c`, `hr_media_pct`, `presion_media_hpa`, `viento_medio_ms`, `rafaga_max_ms`, `radiacion_media_wm2`, `n_lecturas` |
| Métricas de `subcuenca_1h` | `precip_media_mm` (promedio areal), `precip_max_mm`, `estacion_max`, `n_estaciones` |
| Métricas de `alerta` | `tipo` (`lluvia_intensa` o `rafaga_fuerte`), `variable`, `umbral`, `valor` |

Cada pane trae el total acumulado de la ventana. El consumidor hace `UPSERT`
por `aggregate_id` y descarta cualquier pane con un `pane_index` menor al que
ya tiene, que es lo que pasa cuando un reintento llega fuera de orden.

## 4. Política temporal

Es la que diseñé en la Tarea 2, ahora corriendo de verdad:

| Parámetro | Valor | Por qué |
|---|---|---|
| Timestamp | `event_time` | La lluvia pertenece al intervalo en que cayó, y un replay tiene que dar lo mismo. |
| Ventana por estación | fija, 10 min | Es la frecuencia de las estaciones: una lectura por estación y por ventana. |
| Ventana por subcuenca | fija, 1 h | Es la escala que tiene sentido para la lluvia areal. |
| Watermark | `create_time` de KafkaIO (el timestamp es el `event_time`) | El broker guarda el tiempo de evento y Flink propaga el watermark operador por operador. |
| Trigger | `AfterWatermark(early=AfterProcessingTime(10 s), late=AfterCount(1))` | Estimación provisoria, resultado firme y corrección por cada dato tardío. |
| Acumulación | `ACCUMULATING` | Permite hacer `UPSERT` por clave. |
| Lateness | 20 min | Alcanza para las estaciones satelitales cuando andan bien y limita el estado a 30 min por ventana. |
| Deduplicación | `SetState` por `station_id` y ventana, con un timer en fin más lateness | El mismo diseño de la Tarea 3; el timer evita que el estado crezca sin límite. |

## 5. Cómo ejecutarlo

### Todo junto con Docker Compose

Hace falta Docker con Compose y unos 8 GB de memoria libres. Las imágenes de
Beam son `linux/amd64`, así que en Apple Silicon corren emuladas y el primer
arranque tarda más.

```bash
docker compose up --build
```

`dataset-init` baja los datos de Open-Meteo, o genera los sintéticos si no hay
internet (con `HIDROMET_DATASET_SOURCE=sintetico` se fuerza esa opción). Si ya
existe `data/processed/emas_10min.parquet` no lo toca, así que un dataset
preparado a mano (por ejemplo, el de datos reales de la sección 8) se mantiene
y es el que reproduce el productor.

| Interfaz | URL |
|---|---|
| 1. Productor (replay) | <http://localhost:2718> |
| 2. Pipeline Beam (control) | <http://localhost:2719> |
| 3. Consumidor analítico | <http://localhost:2720> |
| Flink Web UI | <http://localhost:8081> |

Primero se lanza el job desde el notebook 2, después el replay desde el
notebook 1, y se mira el notebook 3 (se actualiza cada 2 segundos) junto con
la interfaz de Flink.

```bash
docker compose down            # detener; el log de Kafka queda en el volumen
docker compose down --volumes  # detener y borrar también el log
make smoke                     # prueba corta Kafka -> Beam/Flink -> Kafka
```

### Sin Docker

El proyecto comparte el ambiente virtual del workspace de la raíz del
repositorio (ver `GUIA_UV.md`).

```bash
uv sync --all-packages --frozen
make dataset-sintetico         # o make dataset (Open-Meteo, con respaldo sintético)
make test                      # 18 pruebas
make local                     # pipeline completo en DirectRunner -> data/processed/agregados_local.parquet
```

`scripts/run_local.py` corre exactamente `build_analytics` con los eventos en
el mismo orden en que los publicaría el productor, con atrasos y duplicados
incluidos, pero sin Kafka ni Flink. Como en modo batch el watermark salta
directo al final, no se descarta nada por llegar tarde. Uso esa corrida como
referencia para comparar con la ejecución en streaming.

## 6. Resultados

### Con datos reales

Corrida de referencia (`scripts/run_local.py --max-readings 200000`) sobre las
primeras 200 000 lecturas del dataset de la sección 8: del 1 de julio de 2024
al 7 de enero de 2025, con 9 estaciones.

```
readings: 200000  published: {events: 203960, duplicates: 3960, delayed: 141145}
dlq_records: 2
aggregates: 224341  by_metric_type: {estacion_10min: 200000, subcuenca_1h: 24314, alerta: 27}
```

Salen 200 000 agregados de estación para 200 000 lecturas, cada uno con una
sola lectura, es decir, los 3 960 duplicados no cambiaron ningún total. Las
141 145 lecturas que se publicaron con más de un minuto de atraso cambiaron
el orden de llegada, pero ninguna terminó en una ventana equivocada. Los 2
registros de la DLQ son el JSON roto y la lectura con `temp_c = 99` que el
script mete a propósito.

Comparé la salida con un cálculo aparte en pandas sobre las mismas lecturas,
sin pasar por Beam, y coincide en todo: la lluvia total de cada estación
(5 173,2 mm entre las nueve), las 24 314 combinaciones de subcuenca y hora, y
las 27 lecturas con 10 mm o más, que son las 27 alertas de lluvia intensa.

La salida está en [`docs/evidencia_local_real.txt`](docs/evidencia_local_real.txt),
y la de la prueba de punta a punta con Kafka y Flink (`make smoke`) en
[`docs/evidencia_smoke.txt`](docs/evidencia_smoke.txt).

La referencia no cubre los dos años enteros: el DirectRunner en modo batch
tiene todos los eventos en memoria y con las 792 653 lecturas no termina. Las
cifras del período completo están en la sección 8.

### Con datos sintéticos

Para comprobarlo sin los datos reales, `make dataset-sintetico` y `make local`
corren lo mismo sobre 3 000 lecturas sintéticas, y el resultado es siempre
éste:

```
readings: 3000  published: {events: 3058, duplicates: 58, delayed: 2186}
dlq_records: 2
aggregates: 3255  by_metric_type: {estacion_10min: 3000, subcuenca_1h: 252, alerta: 3}
```

### Pruebas

Las 18 pruebas (`make test`) cubren el contrato (ids deterministas y rechazo
de mensajes inválidos), el `CombineFn`, el pipeline en batch (deduplicación,
agregados por estación y por subcuenca, alertas y DLQ), los adaptadores de
datos (export largo, y CSV con recorte de período y metadatos de la red de
ejemplo) y el consumidor (upsert y descarte de panes viejos). Entre ellas hay cuatro
pruebas con `TestStream` en las que avanzo el watermark a mano: una comprueba
que después del pane ON_TIME llega un LATE acumulado pasando por el `DoFn`
con estado, otra que una lectura que llega pasados los 20 minutos de
lateness se descarta sin tocar el total, otra que un duplicado se ignora, y
la última que la salida publica `pane_timing` con el nombre del pane
(`ON_TIME`, `LATE`) y no con el número interno que usa Beam.

> Algo que encontré con Beam 2.74 en DirectRunner: en modo batch, con
> `ACCUMULATING` y `allowed_lateness` mayor que cero, cada ventana emite dos
> panes con el mismo contenido, el on-time y otro de cierre con
> `is_last=True`. Las pruebas en batch verifican el pane final, y el
> consumidor no se ve afectado porque hace `UPSERT`. En el laboratorio de la
> clase 7 pasa lo mismo.

## 7. Decisiones

Cada punto dice lo que hice, qué alternativa descarté y por qué.

- **Replay acelerado de datos históricos**, en lugar de conectarme a una
  fuente en vivo. Fue la propuesta del profesor para el trabajo final, se
  puede reproducir y me deja provocar los problemas a propósito.
- **Datos reales fuera del repositorio y estaciones anonimizadas**, en lugar
  de usar sólo datos sintéticos. El análisis se hace con mediciones reales,
  que no puedo publicar; las estaciones llevan los códigos y las localidades
  de ejemplo de las Tareas 1 y 2. Cualquiera puede reproducir el proyecto con
  Open-Meteo o con el generador.
- **Atraso según el tipo de enlace**, en lugar de un jitter uniforme para
  todas las estaciones. Se parece más a la realidad (pocas estaciones muy
  atrasadas), y eso es justamente lo que pone a prueba la lateness.
- **Promedio areal por subcuenca**, en lugar de sumar las estaciones. Sumar la
  lluvia de varias estaciones no representa nada físico.
- **`DoFn` con estado y timer para deduplicar**, en lugar de un `GroupByKey`
  para quedarme con el primero. No hay que esperar al trigger y el estado
  tiene un límite claro. Es lo que practiqué en la Tarea 3.
- **Alertas en el mismo tópico que los agregados**, en lugar de un tópico
  aparte. Simplifica la infraestructura. En producción las pondría en un
  tópico propio con retención corta.
- **Dashboard en Marimo**, en lugar de una base de series temporales. Para el
  laboratorio alcanza. El diseño con UPSERT por clave es el mismo que usaría
  con TimescaleDB.

### Particiones

Los tres tópicos tienen 4 particiones. La clave de entrada es la estación, y
en el replay hay entre 8 y 12 estaciones, así que tocan 2 o 3 claves por
partición en promedio. El hash no garantiza un reparto exacto: con tan pocas
claves alguna partición puede quedar con más estaciones que otra, pero todas
emiten con la misma cadencia y no hay claves calientes. El pipeline corre con
paralelismo 2 por defecto, y con 4 particiones puedo subirlo a 4 sin
reparticionar. Más particiones que estaciones no agregarían paralelismo útil.

### Semántica de entrega y límites

No afirmo exactly-once de punta a punta, porque no lo es. Lo que hay es esto:

- El productor del replay y el `WriteToKafka` de los agregados usan
  `enable.idempotence=true` y `acks=all`: un reintento del productor no
  duplica mensajes en el broker.
- La lectura con KafkaIO confirma offsets con `enable.auto.commit`, y en el
  laboratorio no activé checkpoints de Flink. Si un TaskManager se cae, el job
  se reinicia desde el último offset confirmado: algunas lecturas se vuelven
  a procesar (el estado de deduplicación de ese momento se pierde) y algunos
  panes se vuelven a publicar. Es at-least-once hacia `ema.agregados.v1`.
- La vista final es idempotente: el consumidor aplica cada pane con upsert
  por `aggregate_id` e ignora los `pane_index` viejos, y el tópico es
  compactado. Reprocesar o republicar un pane no cambia el resultado visible.
- En producción activaría el checkpointing de Flink (`--checkpointing_interval`)
  y el commit de offsets en la finalización del checkpoint, para que estado y
  offsets se restauren juntos.

Otros límites que conviene tener presentes: una lectura que llega después de
fin de ventana más 20 minutos se descarta, y lo único que queda es el
contador de elementos descartados por atraso del runner (no hay un tópico de
auditoría para los `too_late`, como el que propuse en la Tarea 2); el dashboard vive en
memoria y reconstruye su vista leyendo el tópico compactado desde el
principio en cada sesión; y las alertas comparten tópico con los agregados.

## 8. Datos reales: lluvia de 12 estaciones automáticas

Los datos del análisis son mediciones de lluvia cada 10 minutos de 12
estaciones meteorológicas automáticas de Alto Paraná y Canindeyú. Fuente:
Itaipú Binacional, División de Embalse. La serie completa va de mayo de 2020
a septiembre de 2026 y tiene 2,2 millones de lecturas. Para el trabajo usé
dos años hidrológicos, del 1 de julio de 2024 al 30 de junio de 2026, que son
792 653 lecturas.

Los datos no están en el repositorio y no se publican. Tampoco el nombre ni
la ubicación de las estaciones: cada una lleva un código (`ITA01` a `ITA12`)
y el nombre, la subcuenca, las coordenadas y el tipo de enlace de una
localidad de la red de ejemplo de `dataset.py`, la misma de las Tareas 1 y 2.
Lo que publico son los resultados.

| Estación | Subcuenca | Enlace | Lecturas | Cobertura | Lluvia (mm) | Máx. 10 min (mm) | Ventanas ≥ 10 mm |
|---|---|---|---:|---:|---:|---:|---:|
| `ITA01` Hernandarias | margen-derecha-sur | fibra | 102 365 | 97,4 % | 3 502,9 | 21,4 | 33 |
| `ITA02` Ciudad del Este | margen-derecha-sur | fibra | 102 540 | 97,5 % | 3 804,8 | 18,5 | 30 |
| `ITA03` Presidente Franco | monday | fibra | 22 920 | 21,8 % | 376,0 | 12,3 | 2 |
| `ITA04` Minga Guazú | monday | gprs | 102 603 | 97,6 % | 3 125,3 | 24,3 | 21 |
| `ITA05` Yguazú | acaray | gprs | 21 339 | 20,3 % | 500,9 | 19,3 | 6 |
| `ITA06` Itakyry | acaray | gprs | 86 698 | 82,5 % | 2 393,3 | 15,6 | 5 |
| `ITA07` San Alberto | margen-derecha-centro | gprs | 102 002 | 97,0 % | 3 178,9 | 17,0 | 24 |
| `ITA08` Mbaracayú | margen-derecha-centro | gprs | 98 612 | 93,8 % | 2 369,8 | 21,3 | 16 |
| `ITA09` Nueva Esperanza | margen-derecha-centro | satelital | 16 593 | 15,8 % | 417,0 | 16,5 | 5 |
| `ITA10` Katueté | margen-derecha-norte | satelital | 11 138 | 10,6 % | 333,1 | 9,7 | 0 |
| `ITA11` Salto del Guairá | margen-derecha-norte | satelital | 102 596 | 97,6 % | 3 229,4 | 23,8 | 34 |
| `ITA12` Santa Rita | nacunday | gprs | 23 247 | 22,1 % | 415,9 | 7,9 | 0 |
| **Total** | | | **792 653** | | | **24,3** | **176** |

La cobertura es la cantidad de lecturas sobre las 105 120 que tendría una
estación sin huecos en los dos años. Siete estaciones pasan el 93 %. Las
otras cinco son casos reales de estación que no reporta: `ITA05`, `ITA09` e
`ITA10` empiezan a fines de 2025, e `ITA03` e `ITA12` reportan por tramos. No
rellené nada: el pipeline no emite ventanas sin lecturas, y el agregado por
subcuenca informa en `n_estaciones` cuántas estaciones entraron en cada hora.

Sólo el 3,7 % de las lecturas tiene lluvia (29 520). En los dos años hay 176
ventanas de 10 minutos que llegan al umbral de lluvia intensa (10 mm), y el
máximo es de 24,3 mm en 10 minutos.

La fuente trae sólo la precipitación. Las demás variables del contrato quedan
vacías, que es un caso previsto (sección 3), así que con estos datos los
agregados de estación informan la lluvia y no hay alertas de ráfaga.

Para preparar el dataset a partir del export:

```bash
uv run python -m hidromet_streaming.dataset --source csv --file export.csv \
  --column-map mapa.json --start 2024-07-01 --end 2026-06-30
```

donde `mapa.json` indica qué columna del export corresponde a `station_id`, a
`event_time` y a `precip_mm`. Las columnas que no figuran en el mapa no se
leen, y los metadatos de cada estación salen de la red de ejemplo.

Esto es lo que resuelven los adaptadores (`from_csv` y `from_marr_long`) y
cómo se relaciona con el curso:

- **Fechas en hora local o sin zona.** Las convierto a UTC, así el tiempo de
  evento queda normalizado antes de entrar al log.
- **El valor `-999,99` cuando no hay dato** (export largo). Lo convierto en
  `null`. El contrato acepta variables vacías; la DLQ queda para valores
  imposibles, no para datos faltantes.
- **Filas repetidas en la fuente** (export largo). Me quedo con una y anoto
  cuántas había en el manifiesto. Los duplicados del replay los mete el
  productor a propósito, y el pipeline los elimina.
- **Huecos reales.** Los dejo como están. Una ventana sin lecturas no se
  emite y el dashboard lo muestra como falta de datos.
- **El tipo de enlace de cada estación no viene en los datos.** Uso el de la
  red de ejemplo (fibra, GPRS o satelital), y con eso el replay tiene atrasos
  y desorden para simular.

## 9. Pendiente: machine learning en streaming (clase 8)

No lo implementé en esta entrega. El lugar natural para agregarlo es el
tópico `ema.agregados.v1`: otro consumidor podría detectar anomalías por
estación, por ejemplo una estación cuya lluvia se aleja mucho del promedio de
su subcuenca, o cuya temperatura no se parece a la de sus vecinas, y publicar
`metric_type = "anomalia"` con la misma clave idempotente.

## 10. Estructura

```
src/hidromet_streaming/
  config.py         parámetros (ventanas, lateness, umbrales, tópicos)
  contracts.py      esquema del evento, validación, event_id determinista
  dataset.py        fuentes openmeteo, sintetico, marr y csv -> parquet
  producer.py       replay a Kafka con duplicados y atraso por enlace
  transforms.py     ParseEvent, DeduplicateReadings, CombineFns, alertas, build_analytics
  pipeline.py       Kafka -> Beam (PortableRunner/Flink) -> Kafka, con DLQ
  consumer.py       AggregateStore (upsert) y lectura del tópico
producer_notebook.py, pipeline_notebook.py, analytics_notebook.py
scripts/run_local.py  referencia en batch sin Kafka ni Flink
scripts/smoke.py      prueba corta con Docker
tests/                18 pruebas (pytest)
docker/flink/         imagen de Flink con los SDK de Beam
```

La infraestructura parte del laboratorio de la clase 7
(`rparrapy/fpuna-clase7-taxi-streaming`), que adapté a este dominio.

## 11. Integrantes y uso de IA

Trabajo individual: `abarchello`. Hice el caso de uso, el contrato de eventos,
el replay, el pipeline, las pruebas, la infraestructura con Docker, el
dashboard y la documentación.

Usé herramientas de IA generativa como apoyo durante todo el trabajo: para
redactar y revisar los textos, para escribir y revisar código y pruebas, y
para contrastar decisiones de diseño con lo que vimos en clase. Las decisiones
finales, la ejecución y la verificación son mías, y cada cifra de los
documentos sale de correr el código de este repositorio.

