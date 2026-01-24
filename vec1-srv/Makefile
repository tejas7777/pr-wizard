.PHONY: format run-vec-dev test

format:
	uv run ruff check --fix .
	uv run ruff format .

run-vec-dev:
	uv run python main.py

test:
	uv run pytest
