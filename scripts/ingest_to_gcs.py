import requests
import gzip
import random
import io
import os
from google.cloud import storage
from google.oauth2 import service_account


CRAWL_ID = "CC-MAIN-2026-12"  # latest
MANIFEST_URL = f"https://data.commoncrawl.org/crawl-data/{CRAWL_ID}/wet.paths.gz"
GCS_BUCKET_NAME = "ccdp-raw-common-crawl-deduplication"
SAMPLE_SIZE = 10  # number of files to sample and upload to data lake

# Path to your service account key file (make sure to update this path if your key file is located elsewhere)
SERVICE_ACCOUNT_FILE = os.path.join(
    os.path.dirname(__file__),
    "..",
    "terraform",
    "keys",
    "common-crawl-deduplication-184aa125ef30.json",
)


def get_sampled_paths(url, sample_size):
    """Downloads the manifest and picks random file paths."""

    print(f"[get_sampled_paths] Downloading manifest from {url}...")
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to download manifest: {response.status_code}")
    print(
        f"[get_sampled_paths] Manifest downloaded successfully with status code {response.status_code}."
    )

    # Files are compressed with gzip, so we need to decompress them in memory and split into lines
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
        all_paths = f.read().decode("utf-8").splitlines()

    print(
        f"[get_sampled_paths] Total WET files available: {len(all_paths)}. Sampling {sample_size} files..."
    )
    return random.sample(all_paths, sample_size)


def stream_to_gcs(source_path, bucket_name):
    """Streams a file from Common Crawl URL directly to GCS."""
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE
    )
    storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
    bucket = storage_client.bucket(bucket_name)

    # Common Crawl source URL
    source_url = f"https://data.commoncrawl.org/{source_path}"
    # Target path in GCS (re-creating the folder structure for tidiness)
    target_blob_name = f"raw/{CRAWL_ID}/{source_path.split('/')[-1]}"

    blob = bucket.blob(target_blob_name)
    print(
        f"[stream_to_gcs] Streaming from {source_url} to gs://{bucket_name}/{target_blob_name}..."
    )

    # Use `stream=True` to keep memory usage low.
    """
    The stream=True parameter in a Python requests call prevents the immediate download of the entire response body, 
    allowing you to process large files or continuous data streams in chunks or line-by-line, thus managing memory 
    usage efficiently. 
    """
    with requests.get(source_url, stream=True) as response:
        response.raise_for_status()  # Check for HTTP errors
        # blob.open("wb") is the most efficient way to pipe streams in GCS 2.0+
        with blob.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                if chunk:  # filter out keep-alive chunks
                    f.write(chunk)
    print(
        f"[stream_to_gcs] Successfully streamed {source_url} to gs://{bucket_name}/{target_blob_name}."
    )


def main():
    try:
        # Retrieve random sample
        sample_paths = get_sampled_paths(MANIFEST_URL, SAMPLE_SIZE)

        # Ingest each sampled file one by one to GCS
        for i, path in enumerate(sample_paths):
            print(f"[main] Processing file {i+1}/{SAMPLE_SIZE}: {path}")
            stream_to_gcs(path, GCS_BUCKET_NAME)

        print("\n[main] All files processed successfully. ✅")

    except Exception as e:
        print(f"[main] Error retrieving sampled paths: {e}")
        return
    finally:
        print("[main] Finished retrieving sampled paths.")


if __name__ == "__main__":
    main()
