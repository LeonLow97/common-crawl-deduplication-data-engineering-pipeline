from __future__ import annotations

import gzip
import io
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from google.cloud import storage
from google.oauth2 import service_account


CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Expected a gs:// URI, got: {uri}")

    bucket_name, _, object_path = uri[5:].partition("/")
    if not bucket_name:
        raise ValueError(f"Missing bucket name in GCS URI: {uri}")

    return bucket_name, object_path.strip("/")


def find_service_account_key(
    key_dir: Path,
    key_filename: str | None = None,
) -> Path:
    resolved_key_dir = key_dir.expanduser().resolve()
    if not resolved_key_dir.exists():
        raise FileNotFoundError(f"Key directory does not exist: {resolved_key_dir}")

    if key_filename is not None:
        key_path = resolved_key_dir / key_filename
        if not key_path.exists():
            raise FileNotFoundError(f"Service account key was not found: {key_path}")
        return key_path

    key_files = sorted(path for path in resolved_key_dir.glob("*.json") if path.is_file())
    if not key_files:
        raise FileNotFoundError(f"No service account JSON files found under {resolved_key_dir}")

    if len(key_files) > 1:
        key_list = ", ".join(path.name for path in key_files)
        raise ValueError(
            "Multiple service account JSON files were found. "
            f"Set SERVICE_ACCOUNT_KEY_FILE explicitly. Candidates: {key_list}"
        )

    return key_files[0]


def build_storage_client_from_key_dir(
    key_dir: Path,
    key_filename: str | None = None,
) -> tuple[storage.Client, str | None, Path]:
    key_path = find_service_account_key(key_dir=key_dir, key_filename=key_filename)
    credentials = service_account.Credentials.from_service_account_file(
        str(key_path),
        scopes=[CLOUD_PLATFORM_SCOPE],
    )
    client = storage.Client(project=credentials.project_id, credentials=credentials)
    return client, credentials.project_id, key_path


def list_wet_blobs(
    storage_client: storage.Client,
    gcs_uri: str,
    max_files: int | None = None,
    suffix: str = ".warc.wet.gz",
) -> list[storage.Blob]:
    bucket_name, prefix = parse_gcs_uri(gcs_uri)
    prefix_arg = f"{prefix}/" if prefix else ""
    blobs = [
        blob
        for blob in storage_client.list_blobs(bucket_name, prefix=prefix_arg)
        if blob.name.endswith(suffix) and not blob.name.endswith("/")
    ]
    blobs.sort(key=lambda blob: blob.name)

    if max_files is not None:
        return blobs[:max_files]
    return blobs


@contextmanager
def open_blob_text(blob: storage.Blob) -> Iterator[io.TextIOBase]:
    if hasattr(blob, "open"):
        with blob.open("rb") as raw_stream:
            with gzip.GzipFile(fileobj=raw_stream) as gz_stream:
                with io.TextIOWrapper(gz_stream, encoding="utf-8", errors="ignore") as src:
                    yield src
        return

    compressed = io.BytesIO()
    blob.download_to_file(compressed)
    compressed.seek(0)
    with gzip.GzipFile(fileobj=compressed) as gz_stream:
        with io.TextIOWrapper(gz_stream, encoding="utf-8", errors="ignore") as src:
            yield src


def emit_record(
    lines: list[str],
    source_file: str,
    min_chars: int,
    max_chars: int,
) -> dict[str, str | int] | None:
    if not lines:
        return None

    url = None
    warc_type = None
    body_started = False
    body_lines: list[str] = []

    for line in lines:
        if not body_started:
            if line == "":
                body_started = True
                continue

            if line.startswith("WARC-Type:"):
                warc_type = line.split(":", 1)[1].strip()
            elif line.startswith("WARC-Target-URI:"):
                url = line.split(":", 1)[1].strip()
        else:
            body_lines.append(line)

    if warc_type != "conversion" or not url:
        return None

    text = "\n".join(body_lines).strip()
    if len(text) < min_chars:
        return None

    text = text[:max_chars]

    return {
        "source_file": source_file,
        "url": url,
        "text": text,
        "text_len": len(text),
    }


def build_local_sample_from_gcs(
    blobs: list[storage.Blob],
    output_path: Path,
    max_docs_per_file: int,
    min_chars: int,
    max_chars: int,
) -> tuple[dict[str, int], int]:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows_written = 0
    per_file_counts: dict[str, int] = {}

    with output_path.open("w", encoding="utf-8") as dst:
        for blob in blobs:
            docs_written = 0
            record_lines: list[str] = []
            source_file = Path(blob.name).name

            with open_blob_text(blob) as src:
                for raw_line in src:
                    line = raw_line.rstrip("\n")

                    if line.startswith("WARC/1.0"):
                        row = emit_record(
                            record_lines,
                            source_file=source_file,
                            min_chars=min_chars,
                            max_chars=max_chars,
                        )
                        if row is not None:
                            dst.write(json.dumps(row, ensure_ascii=False) + "\n")
                            docs_written += 1
                            rows_written += 1
                            if docs_written >= max_docs_per_file:
                                break
                        record_lines = [line]
                    else:
                        record_lines.append(line)
                else:
                    row = emit_record(
                        record_lines,
                        source_file=source_file,
                        min_chars=min_chars,
                        max_chars=max_chars,
                    )
                    if row is not None and docs_written < max_docs_per_file:
                        dst.write(json.dumps(row, ensure_ascii=False) + "\n")
                        docs_written += 1
                        rows_written += 1

            per_file_counts[source_file] = docs_written

    return per_file_counts, rows_written


def clear_gcs_prefix(storage_client: storage.Client, gcs_uri: str) -> int:
    bucket_name, prefix = parse_gcs_uri(gcs_uri)
    prefix_arg = f"{prefix}/" if prefix else ""
    delete_count = 0

    for blob in storage_client.list_blobs(bucket_name, prefix=prefix_arg):
        blob.delete()
        delete_count += 1

    return delete_count


def upload_directory_to_gcs(
    storage_client: storage.Client,
    local_dir: Path,
    destination_uri: str,
    clear_prefix: bool = False,
) -> int:
    if not local_dir.exists():
        raise FileNotFoundError(f"Local directory does not exist: {local_dir}")

    if clear_prefix:
        clear_gcs_prefix(storage_client, destination_uri)

    bucket_name, prefix = parse_gcs_uri(destination_uri)
    bucket = storage_client.bucket(bucket_name)
    upload_count = 0

    for local_path in sorted(local_dir.rglob("*")):
        if not local_path.is_file():
            continue

        relative_path = local_path.relative_to(local_dir).as_posix()
        blob_name = "/".join(part for part in [prefix, relative_path] if part)
        bucket.blob(blob_name).upload_from_filename(str(local_path))
        upload_count += 1

    return upload_count
