# Run inside WSL/Linux. pyarrow and PySpark do not install on native ARM64 Windows.
PYTHON ?= python
DATA_DIR ?= data
START ?= 2024-01
END ?= 2024-12

.PHONY: install test lint backfill airflow-up airflow-down

install:
	$(PYTHON) -m pip install -e ".[dev]"

test:
	$(PYTHON) -m pytest

lint:
	ruff check .

# Backfill bronze into local storage, without Airflow.
backfill:
	$(PYTHON) -m lakehouse.ingest --dataset yellow --start $(START) --end $(END) \
		--storage local --root $(DATA_DIR)

airflow-up:
	docker compose -f docker/docker-compose.yml up -d

airflow-down:
	docker compose -f docker/docker-compose.yml down
