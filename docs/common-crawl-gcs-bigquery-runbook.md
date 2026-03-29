# Common Crawl GCS to BigQuery Runbook

This flow runs in three stages:

1. Ingest sampled Common Crawl WET files into GCS.
2. Transform those GCS WET files into deduplicated parquet outputs.
3. Load the final parquet dataset from GCS into BigQuery.

## Files

- `scripts/ingest_to_gcs_threaded.py`: threaded raw WET ingestion into GCS
- `batch/pyspark_batch_gcs.py`: standalone Spark transform converted from the notebook flow
- `scripts/load_gcs_parquet_to_bigquery.py`: BigQuery loader for final parquet files in GCS
- `dags/common_crawl_gcs_to_bigquery_dag.py`: Airflow DAG chaining ingest -> transform -> BigQuery load

## Prerequisites

1. Install `uv`.
2. Make sure your GCP service account key exists at:

```text
terraform/keys/common-crawl-deduplication-184aa125ef30.json
```

3. Ensure the service account has access to:

- read and write the GCS bucket `ccdp-raw-common-crawl-deduplication`
- create datasets/tables and load jobs in BigQuery

## Local Airflow Docker Setup

The Spark transform step needs Java inside the Airflow worker container. This repo now includes:

- `Dockerfile`: extends the Airflow image with OpenJDK 17
- `docker-compose.yaml`: builds that image, sets `JAVA_HOME`, and mounts `batch/` to `/opt/airflow/batch`

Rebuild and restart Airflow after pulling these changes:

```bash
cd /Users/SP15243/Desktop/common-crawl-deduplication-data-engineering-pipeline-main

docker compose down
docker compose build --no-cache
docker compose up airflow-init
docker compose up -d
```

Quick checks inside the worker:

```bash
docker compose exec airflow-worker java -version
docker compose exec airflow-worker bash -lc 'echo $JAVA_HOME'
docker compose exec airflow-worker bash -lc 'ls /opt/airflow/batch'
```

## Direct Script Runs

### 1. Ingest sampled WET files to GCS

```bash
cd /Users/SP15243/Desktop/common-crawl-deduplication-data-engineering-pipeline-main

uv run --with requests --with google-cloud-storage \
  python scripts/ingest_to_gcs_threaded.py \
  --crawl-id CC-MAIN-2026-08 \
  --bucket ccdp-raw-common-crawl-deduplication \
  --sample-size 10 \
  --max-workers 4 \
  --key-file terraform/keys/common-crawl-deduplication-184aa125ef30.json
```

### 2. Transform GCS WET files into parquet

```bash
cd /Users/SP15243/Desktop/common-crawl-deduplication-data-engineering-pipeline-main

uv run --project batch --with google-cloud-storage \
  python batch/pyspark_batch_gcs.py \
  --project-id common-crawl-deduplication \
  --raw-bucket ccdp-raw-common-crawl-deduplication \
  --output-bucket ccdp-raw-common-crawl-deduplication \
  --crawl-id CC-MAIN-2026-08 \
  --key-file common-crawl-deduplication-184aa125ef30.json
```

This writes outputs to:

- `gs://ccdp-raw-common-crawl-deduplication/dedup_outputs/CC-MAIN-2026-08/final_docs`
- `gs://ccdp-raw-common-crawl-deduplication/dedup_outputs/CC-MAIN-2026-08/duplicate_audit`

### 3. Load final parquet files into BigQuery

```bash
cd /Users/SP15243/Desktop/common-crawl-deduplication-data-engineering-pipeline-main

uv run --with google-cloud-storage --with google-cloud-bigquery \
  python scripts/load_gcs_parquet_to_bigquery.py \
  --project-id common-crawl-deduplication \
  --bucket ccdp-raw-common-crawl-deduplication \
  --crawl-id CC-MAIN-2026-08 \
  --dataset-id common_crawl_dedup \
  --table-id final_docs \
  --key-file terraform/keys/common-crawl-deduplication-184aa125ef30.json
```

## Airflow DAG

Use:

- `common_crawl_gcs_to_bigquery_dag`

It runs:

1. `ingest_to_gcs`
2. `transform_gcs_wets`
3. `load_final_docs_to_bigquery`

Example `dag_run.conf`:

```json
{
  "project_id": "common-crawl-deduplication",
  "bucket_name": "ccdp-raw-common-crawl-deduplication",
  "output_bucket_name": "ccdp-raw-common-crawl-deduplication",
  "crawl_id": "CC-MAIN-2026-08",
  "sample_size": 10,
  "max_workers": 4,
  "max_files": 10,
  "max_docs_per_file": 300,
  "dataset_id": "common_crawl_dedup",
  "table_id": "final_docs"
}
```

## Outputs

- GCS raw inputs: `gs://ccdp-raw-common-crawl-deduplication/raw/<crawl_id>/...`
- GCS final parquet: `gs://ccdp-raw-common-crawl-deduplication/dedup_outputs/<crawl_id>/final_docs/...`
- GCS duplicate audit parquet: `gs://ccdp-raw-common-crawl-deduplication/dedup_outputs/<crawl_id>/duplicate_audit/...`
- BigQuery final table: `<project_id>.<dataset_id>.<table_id>`
