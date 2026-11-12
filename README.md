# lakehouse-pipeline

Batch data platform over NYC TLC yellow taxi trip records, with bronze, silver and gold layers, data quality checks and a dashboard.

## Decisions

Dataset: NYC TLC trip records, yellow taxi, monthly parquet files.
Compute: Databricks Free Edition (PySpark, Delta).
Storage: Databricks Unity Catalog volumes.
Orchestration: Apache Airflow in Docker, run in WSL2 or on the homelab.

PySpark and pyarrow do not install on native ARM64 Windows, so run Python inside WSL2.

## Status

| Milestone | State |
|-----------|-------|
| M1 Ingestion (bronze) | done |
| M2 Transformation (silver) | not started |
| M3 Modelling (gold, dbt) | not started |
| M4 Data quality | not started |
| M5 Serving and BI | not started |
| M6 Tie-in with the text-to-SQL agent | not started |

## M1: bronze ingestion

```
TLC CloudFront  ->  HttpDownloader  ->  load_month  ->  Storage (volume or local)
                                            |
                                            +-> manifest JSON (rows, sha256, size)
```

`load_month` downloads one month, records its row count and SHA-256, uploads the file to
`tlc/<dataset>/year=YYYY/month=MM/`, and writes a manifest to `_manifests/` last.
A month counts as loaded only when both the data file and the manifest exist, so a crash between
the two steps is redone on the next run. The data path depends only on dataset and month, so a
redo overwrites the file and never duplicates it.

The Airflow DAG `tlc_bronze_ingest` runs once per month with `catchup=True` and one active run at a
time. The TLC publishes about two months late, so a run retries every six hours until its file exists.

### Run it

```
make install
make test
make backfill START=2024-01 END=2024-12 DATA_DIR=$HOME/lakehouse-data
```

Airflow, with a `docker/.env` that sets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`:

```
make airflow-up
```

Then open http://localhost:8080 (user and password `admin`) and unpause `tlc_bronze_ingest`.

## Layout

```
src/lakehouse/ingest/   download, storage backends, idempotent loader, CLI
dags/                   Airflow DAGs
docker/                 Airflow image and compose file
tests/                  pytest
```

## Limitations

The `DatabricksVolumeStorage` backend has not run against a live workspace yet. It is covered by the
same interface as the local backend, which the tests exercise.
