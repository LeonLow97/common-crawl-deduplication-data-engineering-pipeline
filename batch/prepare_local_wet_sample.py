import argparse
import gzip
import json
from pathlib import Path


def emit_record(lines: list[str], min_chars: int, max_chars: int) -> dict | None:
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

    if len(text) > max_chars:
        text = text[:max_chars]

    return {"url": url, "text": text}


def build_sample(
    input_path: Path,
    output_path: Path,
    max_records: int,
    min_chars: int,
    max_chars: int,
) -> tuple[int, int]:
    written = 0
    seen = 0
    record_lines: list[str] = []

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(input_path, "rt", encoding="utf-8", errors="ignore") as src:
        with output_path.open("w", encoding="utf-8") as dst:
            for raw_line in src:
                line = raw_line.rstrip("\n")

                if line.startswith("WARC/1.0"):
                    row = emit_record(record_lines, min_chars=min_chars, max_chars=max_chars)
                    if row is not None:
                        dst.write(json.dumps(row, ensure_ascii=False) + "\n")
                        written += 1
                        if written >= max_records:
                            break
                    seen += 1
                    record_lines = [line]
                else:
                    record_lines.append(line)
            else:
                row = emit_record(record_lines, min_chars=min_chars, max_chars=max_chars)
                if row is not None and written < max_records:
                    dst.write(json.dumps(row, ensure_ascii=False) + "\n")
                    written += 1
                seen += 1

    return seen, written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a small JSONL sample from a Common Crawl WET gzip file."
    )
    parser.add_argument("input", type=Path, help="Path to the .warc.wet.gz file")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/local_wet_sample.jsonl"),
        help="Where to write the JSONL sample",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=1000,
        help="Maximum number of conversion records to write",
    )
    parser.add_argument(
        "--min-chars",
        type=int,
        default=200,
        help="Skip very short text bodies",
    )
    parser.add_argument(
        "--max-chars",
        type=int,
        default=4000,
        help="Truncate long text bodies to this many characters",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seen, written = build_sample(
        input_path=args.input,
        output_path=args.output,
        max_records=args.max_records,
        min_chars=args.min_chars,
        max_chars=args.max_chars,
    )
    print(
        f"Scanned {seen} WARC records and wrote {written} sampled documents to {args.output}"
    )


if __name__ == "__main__":
    main()
