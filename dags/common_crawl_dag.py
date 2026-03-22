import os
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Get the absolute path to the 'dags' folder where this file lives
# In Composer, this is /home/airflow/gcs/dags
DAG_FOLDER = os.path.dirname(os.path.realpath(__file__))

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="common_crawl_dag",
    default_args=default_args,
    description='Downloads WET files from Common Crawl and streams them to GCS',
    start_date=datetime(2026, 1, 1),
    schedule='@monthly',
    catchup=False,
    tags=['ingestion', 'gcp', 'common_crawl'],
) as dag:

    # 2. Build absolute paths based on your repository structure
    # This points to /your-repo/scripts/ingest_to_gcs.py
    script_path = os.path.join(DAG_FOLDER, "..", "scripts", "ingest_to_gcs.py")
    
    # This points to /your-repo/terraform/keys/common-crawl-deduplication-184aa125ef30.json
    key_path = os.path.join(DAG_FOLDER, "..", "terraform", "keys", "common-crawl-deduplication-184aa125ef30.json")

    run_ingestion = BashOperator(
        task_id='stream_wets_to_gcs',
        bash_command=f"""
            python {script_path} \
                --crawl-id {{{{ dag_run.conf.get('crawl_id', 'CC-MAIN-2026-08') }}}} \
                --bucket {{{{ dag_run.conf.get('bucket_name', 'ccdp-raw-common-crawl-deduplication') }}}} \
                --sample-size {{{{ dag_run.conf.get('sample_size', 10) }}}} \
                --key-file {key_path}
        """
    )
