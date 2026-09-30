.PHONY: install edit run test check docker-run

install:
	uv sync --all-packages  # workspace: nunca uses "uv sync" solo dentro de una subcarpeta

edit:
	uv run marimo edit notebook.py

run:
	uv run marimo run notebook.py

test:
	uv run pytest

check:
	uv run ruff check notebook.py
	uv run marimo check --strict notebook.py

docker-run:
	docker compose up --build notebook
