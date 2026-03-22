import requests
import gzip
import random
import io
import os
import argparse
import sys
from google.cloud import storage
from google.oauth2 import service_account

def get_sampled_paths(url, sample_size):
    """Downloads the manifest and picks random file paths."""

    # IMPORTANT: Using a browser-like User-Agent helps avoid blocks from the Common Crawl CDN
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    print(f"[get_sampled_paths] Downloading manifest from {url}...")
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to download manifest: {response.status_code}. URL: {url}")
    
    print(
        f"[get_sampled_paths] Manifest downloaded successfully with status code {response.status_code}."
    )

    # Files are compressed with gzip, so we need to decompress them in memory and split into lines
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
        all_paths = f.read().decode("utf-8").splitlines()

    print(
        f"[get_sampled_paths] Total WET files available: {len(all_paths)}. Sampling {sample_size} files..."
    )
    
    actual_sample_size = min(len(all_paths), sample_size)
    return random.sample(all_paths, actual_sample_size)


def stream_to_gcs(source_path, bucket_name, crawl_id, key_file):
    """Streams a file from Common Crawl URL directly to GCS."""
    
    # 1. Setup GCS Credentials and Client
    credentials = service_account.Credentials.from_service_account_file(key_file)
    storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
    bucket = storage_client.bucket(bucket_name)

    # 2. Check if bucket exists, create if it doesn't
    bucket = storage_client.bucket(bucket_name)
    if not bucket.exists():
        print(f"[stream_to_gcs] Bucket {bucket_name} not found. Creating it...")
        # Note: You can specify a location like 'US' or 'EUROPE-WEST1'
        storage_client.create_bucket(bucket, location="US")
        print(f"[stream_to_gcs] Bucket {bucket_name} created successfully.")
    else:
        print(f"[stream_to_gcs] Confirmed: Bucket {bucket_name} exists.")

    # 3. Construct the Source URL
    # We strip any leading slashes from the manifest path to avoid double-slash errors
    clean_path = source_path.lstrip('/')
    source_url = f"https://data.commoncrawl.org/{clean_path}"
    
    # 4. Define the Target Path in GCS
    file_name = clean_path.split('/')[-1]
    target_blob_name = f"raw/{crawl_id}/{file_name}"

    blob = bucket.blob(target_blob_name)
    print(f"[stream_to_gcs] Streaming from {source_url} to gs://{bucket_name}/{target_blob_name}...")

    # 5. Stream with Headers
    # CloudFront/Common Crawl may block default Python 'requests' headers
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

    # Use `stream=True` to keep memory usage low.
    """
    The stream=True parameter in a Python requests call prevents the immediate download of the entire response body, 
    allowing you to process large files or continuous data streams in chunks or line-by-line, thus managing memory 
    usage efficiently. 
    """
    with requests.get(source_url, stream=True, headers=headers) as response:
        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code} for {source_url}")
            
        # blob.open("wb") is the most efficient way to pipe streams in GCS 2.0+
        with blob.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                if chunk:  # filter out keep-alive chunks
                    f.write(chunk)
                    
    print(
        f"[stream_to_gcs] Successfully streamed {source_url} to gs://{bucket_name}/{target_blob_name}."
    )


def main():
    # 1. Set up Argument Parsing to receive data from Airflow BashOperator
    parser = argparse.ArgumentParser(description="Common Crawl to GCS Ingestion Tool")
    parser.add_argument("--crawl-id", required=True, help="Common Crawl ID (e.g., CC-MAIN-2026-08)")
    parser.add_argument("--bucket", required=True, help="Target GCS Bucket Name")
    parser.add_argument("--sample-size", type=int, default=10, help="Number of files to sample")
    parser.add_argument("--key-file", required=True, help="Path to the GCP Service Account JSON key")

    args = parser.parse_args()

    # 2. Build the Manifest URL dynamically
    manifest_url = f"https://data.commoncrawl.org/crawl-data/{args.crawl_id}/wet.paths.gz"

    try:
        # 3. Retrieve random sample
        sample_paths = get_sampled_paths(manifest_url, args.sample_size)

        # 4. Ingest each sampled file one by one to GCS
        # Note: If a file 404s, we catch it here to continue with the next file
        # or exit entirely depending on requirements.
        for i, path in enumerate(sample_paths):
            try:
                print(f"[main] Processing file {i+1}/{len(sample_paths)}: {path}")
                stream_to_gcs(path, args.bucket, args.crawl_id, args.key_file)
            except Exception as stream_err:
                print(f"[main] Warning: Skipping file due to error: {stream_err}")
                # Optional: continue to next file
                continue

        print("\n[main] Process complete. ✅")

    except Exception as e:
        # Print the error for logs
        print(f"\n[main] FATAL ERROR: {e}")
        # sys.exit(1) tells Airflow the task failed so it turns RED.
        sys.exit(1)
    finally:
        print("[main] Execution finished.")


if __name__ == "__main__":
    main()