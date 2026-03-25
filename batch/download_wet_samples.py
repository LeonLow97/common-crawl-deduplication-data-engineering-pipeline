import argparse
import gzip
import io
import random
import shutil
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
COMMON_CRAWL_BASE_URL = "https://data.commoncrawl.org"


def fetch_bytes(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request) as response:
        return response.read()


def get_sampled_paths(crawl_id: str, sample_size: int, seed: int) -> list[str]:
    manifest_url = f"{COMMON_CRAWL_BASE_URL}/crawl-data/{crawl_id}/wet.paths.gz"
    print(f"[manifest] Downloading {manifest_url}")

    manifest_bytes = fetch_bytes(manifest_url)
    with gzip.GzipFile(fileobj=io.BytesIO(manifest_bytes)) as gz_file:
        all_paths = gz_file.read().decode("utf-8").splitlines()

    if not all_paths:
        raise RuntimeError(f"No WET paths found in manifest for {crawl_id}")

    actual_sample_size = min(sample_size, len(all_paths))
    print(
        f"[manifest] Found {len(all_paths)} WET files. Sampling {actual_sample_size} using seed {seed}."
    )

    rng = random.Random(seed)
    return rng.sample(all_paths, actual_sample_size)


def download_file(source_path: str, output_dir: Path, index: int, total: int) -> Path:
    clean_path = source_path.lstrip("/")
    source_url = f"{COMMON_CRAWL_BASE_URL}/{clean_path}"
    file_name = clean_path.rsplit("/", 1)[-1]
    output_path = output_dir / file_name

    if output_path.exists():
        print(f"[download] {index}/{total} Skipping existing file {output_path.name}")
        return output_path

    print(f"[download] {index}/{total} Downloading {source_url}")
    request = Request(source_url, headers={"User-Agent": USER_AGENT})
    with urlopen(request) as response, output_path.open("wb") as dst:
        shutil.copyfileobj(response, dst)

    print(f"[download] Saved {output_path}")
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download sampled Common Crawl WET files for local batch work."
    )
    parser.add_argument(
        "--crawl-id",
        default="CC-MAIN-2026-08",
        help="Common Crawl snapshot ID to sample from",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=10,
        help="Number of WET files to download",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible sampling",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/wet_samples"),
        help="Directory where sampled .warc.wet.gz files will be saved",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    try:
        sampled_paths = get_sampled_paths(
            crawl_id=args.crawl_id,
            sample_size=args.sample_size,
            seed=args.seed,
        )

        for index, source_path in enumerate(sampled_paths, start=1):
            download_file(
                source_path=source_path,
                output_dir=args.output_dir,
                index=index,
                total=len(sampled_paths),
            )

        print(
            f"[done] Downloaded {len(sampled_paths)} sampled WET files to {args.output_dir}"
        )
    except (HTTPError, URLError, RuntimeError) as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
