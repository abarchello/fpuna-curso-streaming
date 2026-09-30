# Streaming de datos y sus aplicaciones (MIAAD, FP-UNA)

Mis entregas del curso. Profesor: [`rparrapy`](https://github.com/rparrapy). Autor: `abarchello`.

Todas las entregas trabajan sobre el mismo caso: la telemetría
hidrometeorológica del Alto Paraná, con estaciones automáticas que reportan
cada 10 minutos y la precipitación estimada por satélite, pensando en un año
con El Niño.

| Entrega | Carpeta | Qué contiene | Qué se entrega |
|---|---|---|---|
| Tarea 1: Kafka, logs y arquitectura de eventos | [`tarea1/`](tarea1/) | Caso de uso, tópicos, clave de partición, productores y consumidores, retención y replay, diagrama, evento JSON y una demo opcional con Kafka | PDF |
| Tarea 2: tiempo de evento, ventanas y datos tardíos | [`tarea2/`](tarea2/) | Política temporal aplicada a una secuencia desordenada, con tablas generadas por simulación y una figura | PDF |
| Tarea 3: estado, duplicados e idempotencia con Beam | [`tarea3/`](tarea3/) | Notebook Marimo completo, 19 pruebas pasando (6 con `TestStream`), capturas de ejecución y README con las decisiones | enlace al fork [`abarchello/streaming-fpuna-clase6-tarea`](https://github.com/abarchello/streaming-fpuna-clase6-tarea) |
| Trabajo práctico final | [`trabajo-final/`](trabajo-final/) | Pipeline completo: replay, Kafka, Beam sobre Flink, Kafka y dashboard en Marimo | [`README`](trabajo-final/README.md) |

## Cómo reproducirlo

El repositorio es un workspace de [uv](https://docs.astral.sh/uv/): hay un
solo ambiente virtual (`.venv`, en la raíz) y un solo `uv.lock` para todas
las entregas, en lugar de uno por carpeta. Desde la raíz:

```bash
uv sync --all-packages          # crea o actualiza el ambiente con todo

uv run --directory tarea3 pytest          # pruebas de la Tarea 3
uv run --directory trabajo-final pytest   # pruebas del trabajo final
cd trabajo-final && docker compose up --build   # pila completa con Docker
```

Si nunca usaste uv, en [`GUIA_UV.md`](GUIA_UV.md) está explicado desde cero,
con las equivalencias con pip. Los `Dockerfile` de cada carpeta siguen siendo
independientes: usan el `uv.lock` propio de esa carpeta.

## Evidencia de la Tarea 2

Las tablas y la figura de la Tarea 2 salen de una simulación, así que se
pueden regenerar:

```bash
uv run --directory tarea2 python simulacion.py            # imprime las tablas
uv run --directory tarea2 python generar_doc.py           # arma tarea2/README.md
uv run --directory tarea2 python grafico_linea_tiempo.py  # tarea2/img/linea_tiempo.png
```

## Uso de IA

Usé herramientas de IA generativa como apoyo durante todo el trabajo: para
redactar y revisar los textos, para escribir y revisar código y pruebas, y
para contrastar decisiones de diseño con lo que vimos en clase. Las decisiones
finales, la ejecución y la verificación son mías, y cada cifra de los
documentos sale de correr el código de este repositorio.
