.PHONY: install dataset dataset-sintetico run stop logs test check local smoke clean-data

install:
	uv sync --all-packages --frozen  # workspace compartido con las otras entregas

dataset:
	uv run python -m hidromet_streaming.dataset --source openmeteo --fallback-sintetico

dataset-sintetico:
	uv run python -m hidromet_streaming.dataset --source sintetico --days 3

run:
	docker compose up --build

stop:
	docker compose down

logs:
	docker compose logs --follow --tail=100

test:
	uv run pytest

check:
	uv run ruff check .
	uv run marimo check --strict producer_notebook.py pipeline_notebook.py analytics_notebook.py
	docker compose config --quiet

local:
	uv run python scripts/run_local.py --max-readings 3000

smoke:
	docker compose --profile smoke up --build --abort-on-container-exit --exit-code-from smoke smoke

clean-data:
	find data/cache -type f ! -name .gitkeep -delete
	rm -f data/processed/agregados_local.parquet
