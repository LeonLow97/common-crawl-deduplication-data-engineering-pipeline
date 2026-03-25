# Local Batch Notes

## Why the notebook hits OOM

- `spark.read.text("data/wet_samples/*.gz")` is reading a gzip file, and gzip is not splittable.
- In local mode that means one task ends up scanning one large decompressed stream, so your driver heap does most of the work.
- The sample file in `data/wet_samples/` is about `65 MB` compressed but about `192 MB` after decompression.
- The notebook also keeps very wide string columns alive through `show()`, `dropDuplicates(["text_norm"])`, and later text transforms.

## Safer local workflow

If you want local WET inputs from the `CC-MAIN-2026-08` crawl, first download a small sample set:

```bash
cd batch
python3 download_wet_samples.py \
  --crawl-id CC-MAIN-2026-08 \
  --sample-size 10 \
  --output-dir data/wet_samples
```

For notebook development, first stream the WET file into a small JSONL sample:

```bash
cd batch
python3 prepare_local_wet_sample.py \
  data/wet_samples/CC-MAIN-20260207002457-20260207032457-00195.warc.wet.gz \
  --output data/local_wet_sample.jsonl \
  --max-records 1000 \
  --max-chars 4000
```

Then read that file in Spark instead of the raw `.gz`:

```python
spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("common_crawl_deduplication")
    .config("spark.driver.memory", "6g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

parsed_df = spark.read.json("data/local_wet_sample.jsonl")
```

## Notebook changes that help

- Remove the first exploratory `df_wet = spark.read.text(...)` and avoid `show()` on raw full-text rows.
- In local mode, `spark.executor.memory` does not help much; `spark.driver.memory` is the important knob.
- Keep only the columns you need as early as possible.
- Truncate or sample text before deduplication, not after.
- Keep `limit(...)` before expensive similarity steps.

For the current local notebook defaults, the safer larger-sample settings are:

- `MAX_DOCS_PER_FILE = 300`
- `MAX_FILES = 10`
- `MAX_DOCS_FOR_MINHASH = 2000`
- `SPARK_DRIVER_MEMORY = "8g"`

That gives you more rows per WET file for inspection, while still capping the expensive MinHash stage to a laptop-friendlier size.

The notebook now also reports:

- similarity-band counts for near-duplicate pairs
- cross-domain near-duplicate pair counts
- top domains contributing removed near-duplicate rows

It also writes two Parquet outputs for local batch work:

- `data/parquet/final_docs/` for deduplicated kept documents, partitioned by `crawl_date`
- `data/parquet/duplicate_audit/` for MinHash duplicate-match audit rows
