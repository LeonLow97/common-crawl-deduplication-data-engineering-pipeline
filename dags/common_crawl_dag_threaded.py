import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


DAG_FOLDER = os.path.dirname(os.path.realpath(__file__))

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="common_crawl_dag_threaded",
    default_args=default_args,
    description="Downloads WET files from Common Crawl and streams them to GCS with threaded uploads",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["ingestion", "gcp", "common_crawl", "threaded"],
) as dag:
    script_path = os.path.join(DAG_FOLDER, "..", "scripts", "ingest_to_gcs_threaded.py")
    key_path = os.path.join(
        DAG_FOLDER,
        "..",
        "terraform",
        "keys",
        "common-crawl-deduplication-184aa125ef30.json",
    )

    run_ingestion = BashOperator(
        task_id="stream_wets_to_gcs_threaded",
        bash_command=f"""
            python {script_path} \
                --crawl-id {{{{ dag_run.conf.get('crawl_id', 'CC-MAIN-2026-08') }}}} \
                --bucket {{{{ dag_run.conf.get('bucket_name', 'ccdp-raw-common-crawl-deduplication') }}}} \
                --sample-size {{{{ dag_run.conf.get('sample_size', 10) }}}} \
                --max-workers {{{{ dag_run.conf.get('max_workers', 4) }}}} \
                --key-file {key_path}
        """,
    )
