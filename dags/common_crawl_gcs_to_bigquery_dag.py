import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


DAG_FOLDER = os.path.dirname(os.path.realpath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(DAG_FOLDER, ".."))

INGEST_SCRIPT_PATH = os.path.join(PROJECT_ROOT, "scripts", "ingest_to_gcs_threaded.py")
TRANSFORM_SCRIPT_PATH = os.path.join(PROJECT_ROOT, "batch", "pyspark_batch_gcs.py")
BIGQUERY_LOAD_SCRIPT_PATH = os.path.join(
    PROJECT_ROOT, "scripts", "load_gcs_parquet_to_bigquery.py"
)
KEY_PATH = os.path.join(
    PROJECT_ROOT,
    "terraform",
    "keys",
    "common-crawl-deduplication-XXXXXXXXXXXX.json",
)

DEFAULT_PROJECT_ID = "common-crawl-deduplication"
DEFAULT_BUCKET_NAME = "ccdp-raw-common-crawl-deduplication"
DEFAULT_CRAWL_ID = "CC-MAIN-2026-08"
DEFAULT_DATASET_ID = "common_crawl_dedup"
DEFAULT_TABLE_ID = "final_docs"

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="common_crawl_gcs_to_bigquery_dag",
    default_args=default_args,
    description="Ingest Common Crawl WETs to GCS, transform them to parquet, then load final parquet to BigQuery",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["ingestion", "transform", "bigquery", "common_crawl", "uv"],
) as dag:
    ingest_to_gcs = BashOperator(
        task_id="ingest_to_gcs",
        bash_command=f"""
            cd {PROJECT_ROOT} && \
            uv run --with requests --with google-cloud-storage \
                python {INGEST_SCRIPT_PATH} \
                --crawl-id {{{{ dag_run.conf.get('crawl_id', '{DEFAULT_CRAWL_ID}') }}}} \
                --bucket {{{{ dag_run.conf.get('bucket_name', '{DEFAULT_BUCKET_NAME}') }}}} \
                --sample-size {{{{ dag_run.conf.get('sample_size', 10) }}}} \
                --max-workers {{{{ dag_run.conf.get('max_workers', 4) }}}} \
                --key-file {KEY_PATH}
        """,
    )

    transform_gcs_wets = BashOperator(
        task_id="transform_gcs_wets",
        bash_command=f"""
            cd {PROJECT_ROOT} && \
            uv run --project {os.path.join(PROJECT_ROOT, "batch")} --with google-cloud-storage \
                python {TRANSFORM_SCRIPT_PATH} \
                --project-id {{{{ dag_run.conf.get('project_id', '{DEFAULT_PROJECT_ID}') }}}} \
                --raw-bucket {{{{ dag_run.conf.get('bucket_name', '{DEFAULT_BUCKET_NAME}') }}}} \
                --output-bucket {{{{ dag_run.conf.get('output_bucket_name', '{DEFAULT_BUCKET_NAME}') }}}} \
                --crawl-id {{{{ dag_run.conf.get('crawl_id', '{DEFAULT_CRAWL_ID}') }}}} \
                --key-file common-crawl-deduplication-XXXXXXXXXXXX.json \
                --max-files {{{{ dag_run.conf.get('max_files', 10) }}}} \
                --max-docs-per-file {{{{ dag_run.conf.get('max_docs_per_file', 300) }}}}
        """,
    )

    load_final_docs_to_bigquery = BashOperator(
        task_id="load_final_docs_to_bigquery",
        bash_command=f"""
            cd {PROJECT_ROOT} && \
            uv run --with google-cloud-storage --with google-cloud-bigquery \
                python {BIGQUERY_LOAD_SCRIPT_PATH} \
                --project-id {{{{ dag_run.conf.get('project_id', '{DEFAULT_PROJECT_ID}') }}}} \
                --bucket {{{{ dag_run.conf.get('output_bucket_name', '{DEFAULT_BUCKET_NAME}') }}}} \
                --crawl-id {{{{ dag_run.conf.get('crawl_id', '{DEFAULT_CRAWL_ID}') }}}} \
                --dataset-id {{{{ dag_run.conf.get('dataset_id', '{DEFAULT_DATASET_ID}') }}}} \
                --table-id {{{{ dag_run.conf.get('table_id', '{DEFAULT_TABLE_ID}') }}}} \
                --key-file {KEY_PATH}
        """,
    )

    ingest_to_gcs >> transform_gcs_wets >> load_final_docs_to_bigquery
    # transform_gcs_wets >> load_final_docs_to_bigquery