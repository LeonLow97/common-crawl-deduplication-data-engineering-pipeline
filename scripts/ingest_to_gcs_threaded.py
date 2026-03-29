import argparse
import gzip
import io
import random
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from google.cloud import storage
from google.oauth2 import service_account


COMMON_CRAWL_BASE_URL = "https://data.commoncrawl.org"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
CHUNK_SIZE_BYTES = 1024 * 1024


_thread_local = threading.local()


def get_sampled_paths(url, sample_size, seed):
    """Downloads the manifest and picks random file paths."""
    headers = {"User-Agent": USER_AGENT}

    print(f"[get_sampled_paths] Downloading manifest from {url}...")
    response = requests.get(url, headers=headers, timeout=120)
    response.raise_for_status()

    print(
        f"[get_sampled_paths] Manifest downloaded successfully with status code {response.status_code}."
    )

    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz_file:
        all_paths = gz_file.read().decode("utf-8").splitlines()

    print(
        f"[get_sampled_paths] Total WET files available: {len(all_paths)}. Sampling {sample_size} files..."
    )

    actual_sample_size = min(len(all_paths), sample_size)
    rng = random.Random(seed)
    sampled_paths = rng.sample(all_paths, actual_sample_size)
    sampled_paths.sort()
    print(f"[get_sampled_paths] Using deterministic sample seed: {seed}")
    return sampled_paths


def build_storage_client(key_file):
    credentials = service_account.Credentials.from_service_account_file(key_file)
    return storage.Client(credentials=credentials, project=credentials.project_id)


def ensure_bucket_exists(storage_client, bucket_name):
    bucket = storage_client.bucket(bucket_name)
    if not bucket.exists():
        print(f"[ensure_bucket_exists] Bucket {bucket_name} not found. Creating it...")
        storage_client.create_bucket(bucket, location="US")
        print(f"[ensure_bucket_exists] Bucket {bucket_name} created successfully.")
    else:
        print(f"[ensure_bucket_exists] Confirmed: Bucket {bucket_name} exists.")


def clear_gcs_prefix(storage_client, bucket_name, prefix):
    normalized_prefix = prefix.strip("/")
    if not normalized_prefix:
        raise ValueError("Refusing to clear an empty GCS prefix")

    bucket = storage_client.bucket(bucket_name)
    blobs = list(storage_client.list_blobs(bucket, prefix=f"{normalized_prefix}/"))

    if not blobs:
        print(
            f"[clear_gcs_prefix] No existing objects found under gs://{bucket_name}/{normalized_prefix}/"
        )
        return

    print(
        f"[clear_gcs_prefix] Deleting {len(blobs)} existing objects under "
        f"gs://{bucket_name}/{normalized_prefix}/ before upload."
    )
    bucket.delete_blobs(blobs)
    print(
        f"[clear_gcs_prefix] Cleared gs://{bucket_name}/{normalized_prefix}/ successfully."
    )


def get_thread_storage_client(key_file):
    client = getattr(_thread_local, "storage_client", None)
    if client is None:
        client = build_storage_client(key_file)
        _thread_local.storage_client = client
    return client


def get_thread_requests_session():
    session = getattr(_thread_local, "requests_session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        _thread_local.requests_session = session
    return session


def stream_to_gcs(source_path, bucket_name, crawl_id, key_file):
    """Streams a file from Common Crawl URL directly to GCS."""
    storage_client = get_thread_storage_client(key_file)
    session = get_thread_requests_session()

    clean_path = source_path.lstrip("/")
    source_url = f"{COMMON_CRAWL_BASE_URL}/{clean_path}"
    file_name = clean_path.split("/")[-1]
    target_blob_name = f"raw/{crawl_id}/{file_name}"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(target_blob_name)

    print(
        f"[stream_to_gcs] Thread {threading.current_thread().name} streaming "
        f"{source_url} to gs://{bucket_name}/{target_blob_name}..."
    )

    with session.get(source_url, stream=True, timeout=(30, 600)) as response:
        response.raise_for_status()
        with blob.open("wb") as dst:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE_BYTES):
                if chunk:
                    dst.write(chunk)

    print(
        f"[stream_to_gcs] Successfully streamed {source_url} to "
        f"gs://{bucket_name}/{target_blob_name}."
    )
    return target_blob_name


def main():
    parser = argparse.ArgumentParser(
        description="Common Crawl to GCS ingestion tool with threaded uploads."
    )
    parser.add_argument(
        "--crawl-id",
        required=True,
        help="Common Crawl ID (e.g., CC-MAIN-2026-08)",
    )
    parser.add_argument("--bucket", required=True, help="Target GCS bucket name")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=10,
        help="Number of files to sample",
    )
    parser.add_argument(
        "--key-file",
        required=True,
        help="Path to the GCP service account JSON key",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Number of parallel upload threads to run",
    )
    parser.add_argument(
        "--sample-seed",
        default=None,
        help="Optional deterministic seed for manifest sampling. Defaults to a crawl-specific seed.",
    )
    parser.add_argument(
        "--keep-existing-prefix",
        action="store_true",
        help="Keep existing objects under raw/<crawl-id>/ instead of clearing them before upload.",
    )

    args = parser.parse_args()
    if args.max_workers < 1:
        raise ValueError("--max-workers must be at least 1")

    manifest_url = f"{COMMON_CRAWL_BASE_URL}/crawl-data/{args.crawl_id}/wet.paths.gz"
    sample_seed = args.sample_seed or f"{args.crawl_id}:{args.sample_size}"
    raw_prefix = f"raw/{args.crawl_id}"

    try:
        sample_paths = get_sampled_paths(manifest_url, args.sample_size, sample_seed)

        bootstrap_client = build_storage_client(args.key_file)
        ensure_bucket_exists(bootstrap_client, args.bucket)
        if args.keep_existing_prefix:
            print(
                f"[main] Keeping existing objects under gs://{args.bucket}/{raw_prefix}/."
            )
        else:
            clear_gcs_prefix(bootstrap_client, args.bucket, raw_prefix)

        print(
            f"[main] Starting threaded upload for {len(sample_paths)} files "
            f"with max_workers={args.max_workers} into gs://{args.bucket}/{raw_prefix}/."
        )

        success_count = 0
        failure_count = 0

        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            future_to_path = {
                executor.submit(
                    stream_to_gcs,
                    path,
                    args.bucket,
                    args.crawl_id,
                    args.key_file,
                ): path
                for path in sample_paths
            }

            for index, future in enumerate(as_completed(future_to_path), start=1):
                path = future_to_path[future]
                try:
                    target_blob_name = future.result()
                    success_count += 1
                    print(
                        f"[main] Completed {index}/{len(sample_paths)}: "
                        f"{path} -> gs://{args.bucket}/{target_blob_name}"
                    )
                except Exception as stream_err:
                    failure_count += 1
                    print(f"[main] Warning: Failed to upload {path}: {stream_err}")

        print(
            f"\n[main] Process complete. Successes={success_count}, failures={failure_count}. ✅"
        )

    except Exception as exc:
        print(f"\n[main] FATAL ERROR: {exc}")
        sys.exit(1)
    finally:
        print("[main] Execution finished.")


if __name__ == "__main__":
    main()
