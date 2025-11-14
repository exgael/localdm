.PHONY: install lint lint-fix format typecheck all

install:
	uv sync

lint:
	uv run ruff check .

lint-fix:
	uv run ruff check . --fix --unsafe-fixes

format:
	uv run ruff format .

typecheck:
	uv run mypy . \
	--strict \
	--disallow-any-unimported \
	--disallow-any-decorated \
	--no-implicit-reexport \
	--strict-equality

all: install format lint-fix typecheck

build:
	rm -rf dist && uv build

