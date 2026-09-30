# Demo opcional: productor y consumidor

Kafka 4.1.1 en modo KRaft, con la imagen oficial `apache/kafka` que el
profesor compartió en la clase 2 (la misma versión que usa el trabajo final).
Los tópicos de la Tarea 1 se crean solos al levantar.

```bash
docker compose up -d            # levanta Kafka y crea los tópicos
docker compose logs init-topics # verifica la lista de tópicos
```

## Con las herramientas de consola

Es el mismo camino del quickstart de la clase 2, pero con los tópicos y la
clave de la tarea. La salida completa de una corrida está en
[`evidencia_ejecucion.txt`](evidencia_ejecucion.txt).

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh   --bootstrap-server kafka:29092 --topic ema.lecturas.raw   --property parse.key=true --property "key.separator=|"
# pegar líneas "estación|json", por ejemplo:
# ITA07|{"event_id":"ema-ITA07-20260924T141000Z","station_id":"ITA07",...}

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh   --bootstrap-server kafka:29092 --topic ema.lecturas.raw --group demo-consola   --from-beginning --property print.partition=true --property print.offset=true   --property print.key=true
```

En la evidencia mandé cinco lecturas: dos de ITA07, un reenvío de la segunda
(mismo `event_id`), una de ITA12 y una de ITA23 que llega media hora tarde.
Las tres de ITA07 caen en la misma partición y en orden, la de ITA23 se lee
en el orden en que llegó y no en el de su `event_time`, y después de
reposicionar el grupo con `--reset-offsets --to-earliest` se vuelve a leer
todo desde el offset 0.

## Con Python

```bash

python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python producer.py   # terminal 1: 4 estaciones, tiempo acelerado, duplicados y atrasos
python consumer.py   # terminal 2: imprime partición, offset, atraso y duplicados
```

Si ya tenés el ambiente de uv del repositorio (`uv sync --all-packages` en la
raíz), no hace falta el venv: desde esta carpeta alcanza con
`uv run python producer.py` y `uv run python consumer.py`.

El cliente es `kafka-python` 2.x. Probé antes con el fork `kafka-python-ng`,
pero no reconoce `enable_idempotence` y el productor no arranca.

Para probar el replay, detené el consumidor y reposicioná su grupo:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --group demo-consumer \
  --topic ema.lecturas.raw --reset-offsets --to-earliest --execute
python consumer.py   # vuelve a leer todo desde el inicio del log
```

Observá que las lecturas de una misma estación caen siempre en la misma
partición (clave = `station_id`) y que un mensaje "atrasado" se publica
después de otros con `event_time` posterior: Kafka conserva el orden de
publicación, no el de tiempo de evento.
