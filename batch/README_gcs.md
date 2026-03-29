# GCS-backed Batch Notes

This keeps the existing local notebook untouched and adds a separate GCS-focused workflow.

## Files

- `pyspark_batch_gcs.ipynb`: notebook variant that reads sampled `.warc.wet.gz` files from GCS and uploads parquet outputs back to GCS
- `gcs_storage_helpers.py`: helpers for service-account auth, blob listing, sample streaming, and recursive uploads
- `requirements-gcs.txt`: extra Python dependency needed for the GCS helpers

## Setup

```sh
brew install openjdk@17
export JAVA_HOME=$(brew --prefix openjdk@17)
export PATH="$JAVA_HOME/bin:$PATH"
java --version

uv init
uv add requests google-cloud-storage pyspark pandas jupyter
uv run jupyter notebook
```

1. Install the extra dependency:

```bash
cd batch
python3 -m pip install -r requirements-gcs.txt
```

2. Put exactly one GCP service-account JSON file under `./keys/`, or set `SERVICE_ACCOUNT_KEY_FILE` in the notebook if that folder contains multiple keys.

3. In `pyspark_batch_gcs.ipynb`, set:

- `GCS_INPUT_WET_URI`, for example `gs://my-input-bucket/commoncrawl/wet_samples`
- `GCS_OUTPUT_BASE_URI`, for example `gs://my-output-bucket/commoncrawl/dedup`

## Note

The GCS notebook stages parquet locally before upload so local Spark does not need the Hadoop GCS connector.
