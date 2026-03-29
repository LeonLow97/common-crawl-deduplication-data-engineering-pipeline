from __future__ import annotations

import argparse
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.oauth2 import service_account


DEFAULT_PROJECT_ID = "common-crawl-deduplication"
DEFAULT_BUCKET_NAME = "ccdp-raw-common-crawl-deduplication"
DEFAULT_CRAWL_ID = "CC-MAIN-2026-08"
DEFAULT_SERVICE_ACCOUNT_KEY_FILE = "terraform/keys/common-crawl-deduplication-XXXXXXXXXXXX.json"
DEFAULT_DATASET_ID = "common_crawl_dedup"
DEFAULT_TABLE_ID = "final_docs"
DEFAULT_LOCATION = "US"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load final Parquet files from GCS into BigQuery."
    )
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET_NAME)
    parser.add_argument("--crawl-id", default=DEFAULT_CRAWL_ID)
    parser.add_argument(
        "--gcs-prefix",
        default=None,
        help="GCS prefix that contains final parquet files. Defaults to dedup_outputs/<crawl-id>/final_docs.",
    )
    parser.add_argument("--dataset-id", default=DEFAULT_DATASET_ID)
    parser.add_argument("--table-id", default=DEFAULT_TABLE_ID)
    parser.add_argument("--location", default=DEFAULT_LOCATION)
    parser.add_argument(
        "--key-file",
        default=DEFAULT_SERVICE_ACCOUNT_KEY_FILE,
        help="Path to the service account JSON key file.",
    )
    parser.add_argument(
        "--write-disposition",
        default=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    parser.add_argument(
        "--create-disposition",
        default=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    parser.add_argument(
        "--time-partition-field",
        default="crawl_date",
        help="Optional DATE/TIMESTAMP field to partition the destination table by.",
    )
    return parser.parse_args()


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// URI, got {uri}")

    bucket_name, _, prefix = uri[5:].partition("/")
    return bucket_name, prefix.strip("/")


def build_clients(key_file: str, project_id: str) -> tuple[storage.Client, bigquery.Client]:
    key_path = Path(key_file).expanduser().resolve()
    credentials = service_account.Credentials.from_service_account_file(str(key_path))
    storage_client = storage.Client(credentials=credentials, project=project_id)
    bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
    return storage_client, bigquery_client


def list_parquet_uris(storage_client: storage.Client, gcs_uri: str) -> list[str]:
    bucket_name, prefix = parse_gcs_uri(gcs_uri)
    prefix_arg = f"{prefix.rstrip('/')}/" if prefix else ""
    parquet_uris = [
        f"gs://{bucket_name}/{blob.name}"
        for blob in storage_client.list_blobs(bucket_name, prefix=prefix_arg)
        if blob.name.endswith(".parquet")
    ]
    parquet_uris.sort()

    if not parquet_uris:
        raise FileNotFoundError(f"No parquet files found under {gcs_uri}")

    return parquet_uris


def ensure_dataset(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    location: str,
) -> None:
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = location

    try:
        bigquery_client.get_dataset(dataset_ref)
        print(f"[ensure_dataset] Dataset {project_id}.{dataset_id} already exists.")
    except NotFound:
        bigquery_client.create_dataset(dataset_ref)
        print(f"[ensure_dataset] Created dataset {project_id}.{dataset_id}.")


def load_parquet_to_bigquery(
    bigquery_client: bigquery.Client,
    source_uris: list[str],
    destination_table: str,
    location: str,
    write_disposition: str,
    create_disposition: str,
    time_partition_field: str | None,
) -> bigquery.job.LoadJob:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        create_disposition=create_disposition,
        autodetect=True,
    )

    if time_partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            field=time_partition_field
        )

    load_job = bigquery_client.load_table_from_uri(
        source_uris,
        destination_table,
        location=location,
        job_config=job_config,
    )
    load_job.result()
    return load_job


def main() -> None:
    args = parse_args()
    gcs_prefix = args.gcs_prefix or f"dedup_outputs/{args.crawl_id}/final_docs"
    gcs_uri = f"gs://{args.bucket}/{gcs_prefix.strip('/')}"
    destination_table = f"{args.project_id}.{args.dataset_id}.{args.table_id}"

    storage_client, bigquery_client = build_clients(args.key_file, args.project_id)
    parquet_uris = list_parquet_uris(storage_client, gcs_uri)

    print(f"[main] Found {len(parquet_uris)} parquet files under {gcs_uri}.")
    ensure_dataset(
        bigquery_client,
        args.project_id,
        args.dataset_id,
        args.location,
    )

    load_job = load_parquet_to_bigquery(
        bigquery_client,
        parquet_uris,
        destination_table,
        args.location,
        args.write_disposition,
        args.create_disposition,
        args.time_partition_field or None,
    )

    table = bigquery_client.get_table(destination_table)
    print(f"[main] Loaded parquet files into {destination_table}.")
    print(f"[main] BigQuery job id: {load_job.job_id}")
    print(f"[main] Loaded rows: {table.num_rows}")


if __name__ == "__main__":
    main()
