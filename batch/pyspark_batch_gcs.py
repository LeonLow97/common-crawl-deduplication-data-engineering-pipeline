from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from pyspark.ml.feature import HashingTF, MinHashLSH, NGram, RegexTokenizer
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


BATCH_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BATCH_DIR.parent
if str(BATCH_DIR) not in sys.path:
    sys.path.append(str(BATCH_DIR))

from gcs_storage_helpers import (  # noqa: E402
    build_local_sample_from_gcs,
    build_storage_client_from_key_dir,
    list_wet_blobs,
    upload_directory_to_gcs,
)


DEFAULT_PROJECT_ID = "common-crawl-deduplication"
DEFAULT_RAW_BUCKET_NAME = "ccdp-raw-common-crawl-deduplication"
DEFAULT_CRAWL_ID = "CC-MAIN-2026-08"
DEFAULT_SERVICE_ACCOUNT_KEY_FILE = "common-crawl-deduplication-XXXXXXXXXXXX.json"

DEFAULT_MAX_FILES = 10
DEFAULT_MAX_DOCS_PER_FILE = 300
DEFAULT_MIN_TEXT_CHARS = 200
DEFAULT_MAX_TEXT_CHARS = 4_000
DEFAULT_MAX_DOCS_FOR_MINHASH = 2_000
DEFAULT_JACCARD_DISTANCE_THRESHOLD = 0.25
DEFAULT_NGRAM_SIZE = 2
DEFAULT_SPARK_DRIVER_MEMORY = "8g"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the GCS-backed Common Crawl Spark transform as a Python script."
    )
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    parser.add_argument("--raw-bucket", default=DEFAULT_RAW_BUCKET_NAME)
    parser.add_argument("--output-bucket", default=DEFAULT_RAW_BUCKET_NAME)
    parser.add_argument("--crawl-id", default=DEFAULT_CRAWL_ID)
    parser.add_argument(
        "--input-prefix",
        default=None,
        help="GCS object prefix holding raw WET objects. Defaults to raw/<crawl-id>.",
    )
    parser.add_argument(
        "--output-prefix",
        default=None,
        help="GCS object prefix for transform outputs. Defaults to dedup_outputs/<crawl-id>.",
    )
    parser.add_argument(
        "--key-file",
        default=DEFAULT_SERVICE_ACCOUNT_KEY_FILE,
        help="Service account JSON filename inside keys/.",
    )
    parser.add_argument("--max-files", type=int, default=DEFAULT_MAX_FILES)
    parser.add_argument(
        "--max-docs-per-file", type=int, default=DEFAULT_MAX_DOCS_PER_FILE
    )
    parser.add_argument("--min-text-chars", type=int, default=DEFAULT_MIN_TEXT_CHARS)
    parser.add_argument("--max-text-chars", type=int, default=DEFAULT_MAX_TEXT_CHARS)
    parser.add_argument(
        "--max-docs-for-minhash", type=int, default=DEFAULT_MAX_DOCS_FOR_MINHASH
    )
    parser.add_argument(
        "--jaccard-distance-threshold",
        type=float,
        default=DEFAULT_JACCARD_DISTANCE_THRESHOLD,
    )
    parser.add_argument("--ngram-size", type=int, default=DEFAULT_NGRAM_SIZE)
    parser.add_argument(
        "--spark-driver-memory", default=DEFAULT_SPARK_DRIVER_MEMORY
    )
    parser.add_argument(
        "--keep-local-parquet-stage",
        action="store_true",
        help="Keep the local parquet staging directory after uploads complete.",
    )
    return parser.parse_args()


def resolve_key_dir() -> Path:
    candidates = [
        PROJECT_ROOT / "terraform" / "keys",
        PROJECT_ROOT / "keys",
        BATCH_DIR / "keys",
    ]
    return next((path for path in candidates if path.exists()), candidates[0])


def build_gcs_uris(args: argparse.Namespace) -> tuple[str, str]:
    input_prefix = (args.input_prefix or f"raw/{args.crawl_id}").strip("/")
    output_prefix = (args.output_prefix or f"dedup_outputs/{args.crawl_id}").strip("/")
    input_uri = f"gs://{args.raw_bucket}/{input_prefix}"
    output_uri = f"gs://{args.output_bucket}/{output_prefix}"
    return input_uri, output_uri


def build_spark_session(driver_memory: str) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("common_crawl_deduplication_gcs")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def run_transform(args: argparse.Namespace) -> None:
    key_dir = resolve_key_dir()
    gcs_input_wet_uri, gcs_output_base_uri = build_gcs_uris(args)

    local_sample_path = BATCH_DIR / "data" / "local_wet_sample.jsonl"
    local_parquet_stage_dir = BATCH_DIR / "data" / "parquet_stage"
    final_parquet_stage_path = local_parquet_stage_dir / "final_docs"
    duplicate_audit_stage_path = local_parquet_stage_dir / "duplicate_audit"

    storage_client, gcp_project_id, service_account_key_path = (
        build_storage_client_from_key_dir(key_dir, key_filename=args.key_file)
    )
    wet_blobs = list_wet_blobs(
        storage_client,
        gcs_input_wet_uri,
        max_files=args.max_files,
    )

    if not wet_blobs:
        raise FileNotFoundError(f"No WET gzip files found under {gcs_input_wet_uri}")

    print(f"GCP project: {gcp_project_id or args.project_id}")
    print(f"Using service account key: {service_account_key_path}")
    print(f"Using {len(wet_blobs)} WET files from {gcs_input_wet_uri}:")
    for blob in wet_blobs:
        print(" -", Path(blob.name).name)

    per_file_counts, total_rows = build_local_sample_from_gcs(
        wet_blobs,
        local_sample_path,
        max_docs_per_file=args.max_docs_per_file,
        min_chars=args.min_text_chars,
        max_chars=args.max_text_chars,
    )
    print(f"Wrote {total_rows} sampled documents to {local_sample_path}")
    print("Per-file sampled counts:", per_file_counts)

    spark = build_spark_session(args.spark_driver_memory)
    try:
        docs_df = spark.read.json(str(local_sample_path))

        docs_df = (
            docs_df.withColumn(
                "domain", F.regexp_extract("url", r"https?://([^/]+)", 1)
            )
            .filter(F.col("text_len") >= args.min_text_chars)
            .filter(~F.col("url").rlike("(?i).*(porn|xxx|adult).*"))
        )

        print("rows:", docs_df.count())
        docs_df.groupBy("source_file").count().orderBy("source_file").show(
            truncate=False
        )

        docs_clean = (
            docs_df.select("source_file", "domain", "url", "text")
            .withColumn("text_norm", F.lower(F.col("text")))
            .withColumn("text_norm", F.regexp_replace("text_norm", r"https?://\\S+", " "))
            .withColumn("text_norm", F.regexp_replace("text_norm", r"www\\.\\S+", " "))
            .withColumn("text_norm", F.regexp_replace("text_norm", r"[^a-z0-9\\s]", " "))
            .withColumn("text_norm", F.regexp_replace("text_norm", r"\\b\\d+\\b", " "))
            .withColumn("text_norm", F.regexp_replace("text_norm", r"\s+", " "))
            .withColumn("text_norm", F.trim("text_norm"))
            .withColumn(
                "text_norm",
                F.substring("text_norm", 1, args.max_text_chars),
            )
            .filter(F.length("text_norm") >= args.min_text_chars)
        )

        exact_duplicate_examples = (
            docs_clean.groupBy("text_norm")
            .agg(
                F.count("*").alias("doc_count"),
                F.collect_list(F.struct("source_file", "url")).alias("docs"),
            )
            .filter(F.col("doc_count") > 1)
            .orderBy(F.desc("doc_count"), F.asc("text_norm"))
        )

        before_exact = docs_clean.count()
        exact_dedup = docs_clean.dropDuplicates(["text_norm"]).cache()
        after_exact = exact_dedup.count()

        print("before exact dedup:", before_exact)
        print("after exact dedup:", after_exact)
        print("exact duplicate groups:", exact_duplicate_examples.count())
        exact_duplicate_examples.select("doc_count", "docs").show(10, truncate=100)

        minhash_input = (
            exact_dedup.orderBy("source_file", "url")
            .limit(args.max_docs_for_minhash)
            .withColumn("doc_id", F.monotonically_increasing_id())
        )

        tokenizer = RegexTokenizer(
            inputCol="text_norm",
            outputCol="tokens",
            pattern=r"\W+",
            gaps=True,
            minTokenLength=2,
        )
        ngram = NGram(n=args.ngram_size, inputCol="tokens", outputCol="shingles")
        hashing_tf = HashingTF(
            inputCol="shingles",
            outputCol="features",
            numFeatures=1 << 16,
            binary=True,
        )

        tokenized = tokenizer.transform(minhash_input)
        shingled = ngram.transform(tokenized).filter(F.size("shingles") > 0)
        features = (
            hashing_tf.transform(shingled)
            .select("doc_id", "source_file", "domain", "url", "text_norm", "features")
            .cache()
        )

        print("docs going into MinHash:", features.count())

        mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=4)
        model = mh.fit(features)

        pairs = (
            model.approxSimilarityJoin(
                features.alias("a"),
                features.alias("b"),
                args.jaccard_distance_threshold,
                distCol="jaccard_distance",
            )
            .select(
                F.col("datasetA.doc_id").alias("doc_id_a"),
                F.col("datasetB.doc_id").alias("doc_id_b"),
                F.col("datasetA.source_file").alias("source_file_a"),
                F.col("datasetB.source_file").alias("source_file_b"),
                F.col("datasetA.domain").alias("domain_a"),
                F.col("datasetB.domain").alias("domain_b"),
                F.col("datasetA.url").alias("url_a"),
                F.col("datasetB.url").alias("url_b"),
                F.col("jaccard_distance"),
                (F.lit(1.0) - F.col("jaccard_distance")).alias("jaccard_similarity"),
                F.substring(F.col("datasetA.text_norm"), 1, 140).alias("preview_a"),
                F.substring(F.col("datasetB.text_norm"), 1, 140).alias("preview_b"),
            )
            .filter(F.col("doc_id_a") < F.col("doc_id_b"))
            .orderBy(F.col("jaccard_distance").asc(), F.col("url_a").asc())
        )

        print("candidate near-duplicate pairs:", pairs.count())
        pairs.show(50, truncate=80)

        best_match_window = Window.partitionBy("doc_id_b").orderBy(
            F.col("jaccard_distance").asc(),
            F.col("url_a").asc(),
        )

        duplicate_matches = (
            pairs.withColumn("pair_rank", F.row_number().over(best_match_window))
            .filter(F.col("pair_rank") == 1)
            .cache()
        )

        duplicate_ids = (
            duplicate_matches.select(F.col("doc_id_b").alias("doc_id")).distinct()
        )
        deduped_docs = minhash_input.join(
            duplicate_ids, on="doc_id", how="left_anti"
        ).cache()

        removed_docs = (
            minhash_input.alias("removed")
            .join(
                duplicate_matches.alias("match"),
                F.col("removed.doc_id") == F.col("match.doc_id_b"),
            )
            .select(
                F.col("match.jaccard_similarity"),
                F.col("match.source_file_a").alias("kept_source_file"),
                F.col("match.url_a").alias("kept_url"),
                F.col("removed.source_file").alias("removed_source_file"),
                F.col("removed.url").alias("removed_url"),
                F.substring(F.col("removed.text_norm"), 1, 140).alias(
                    "removed_preview"
                ),
            )
            .orderBy(F.col("jaccard_similarity").desc(), F.col("removed_url").asc())
        )

        print("near-duplicate docs removed:", removed_docs.count())
        removed_docs.show(20, truncate=80)
        print("docs after MinHash-based filtering:", deduped_docs.count())
        deduped_docs.select("source_file", "domain", "url").show(20, truncate=80)

        similarity_bands = (
            pairs.withColumn(
                "similarity_band",
                F.when(F.col("jaccard_similarity") >= 0.95, F.lit("0.95-1.00"))
                .when(F.col("jaccard_similarity") >= 0.90, F.lit("0.90-0.95"))
                .when(F.col("jaccard_similarity") >= 0.85, F.lit("0.85-0.90"))
                .otherwise(F.lit("0.75-0.85")),
            )
            .groupBy("similarity_band")
            .agg(F.count("*").alias("pair_count"))
            .orderBy(F.desc("similarity_band"))
        )

        duplicate_domains = (
            removed_docs.withColumn(
                "removed_domain",
                F.regexp_extract("removed_url", r"https?://([^/]+)", 1),
            )
            .groupBy("removed_domain")
            .agg(F.count("*").alias("removed_docs"))
            .orderBy(F.desc("removed_docs"), F.asc("removed_domain"))
        )

        cross_domain_pairs = pairs.filter(F.col("domain_a") != F.col("domain_b")).count()

        print("cross-domain near-duplicate pairs:", cross_domain_pairs)
        similarity_bands.show(truncate=False)
        duplicate_domains.show(20, truncate=False)

        scored_features = (
            model.transform(features)
            .withColumn(
                "minhash_signature_json",
                F.to_json(F.transform("hashes", lambda x: vector_to_array(x))),
            )
            .select("doc_id", "minhash_signature_json")
        )

        final_docs = (
            deduped_docs.alias("d")
            .join(
                docs_df.select("source_file", "url", "text_len").alias("raw"),
                on=["source_file", "url"],
                how="left",
            )
            .join(scored_features.alias("s"), on="doc_id", how="left")
            .withColumn("crawl_id", F.lit(args.crawl_id))
            .withColumn(
                "file_timestamp_raw",
                F.regexp_extract("source_file", r"CC-MAIN-(\d{14})-", 1),
            )
            .withColumn(
                "crawl_file_timestamp",
                F.expr(
                    "try_to_timestamp(nullif(file_timestamp_raw, ''), 'yyyyMMddHHmmss')"
                ),
            )
            .withColumn("crawl_date", F.to_date("crawl_file_timestamp"))
            .withColumn("minhash_num_tables", F.lit(4))
            .withColumn("dedup_method", F.lit("exact_plus_minhash"))
            .withColumn("record_status", F.lit("kept"))
            .select(
                "crawl_id",
                "crawl_date",
                "crawl_file_timestamp",
                "doc_id",
                "source_file",
                "domain",
                "url",
                "text",
                "text_len",
                "text_norm",
                "minhash_signature_json",
                "minhash_num_tables",
                "dedup_method",
                "record_status",
            )
        )

        duplicate_audit = (
            duplicate_matches.withColumn("crawl_id", F.lit(args.crawl_id))
            .withColumn("match_rank", F.lit(1))
            .select(
                "crawl_id",
                "doc_id_a",
                "doc_id_b",
                "source_file_a",
                "source_file_b",
                "domain_a",
                "domain_b",
                "url_a",
                "url_b",
                "jaccard_distance",
                "jaccard_similarity",
                "match_rank",
            )
        )

        final_parquet_gcs_uri = f"{gcs_output_base_uri.rstrip('/')}/final_docs"
        duplicate_audit_gcs_uri = f"{gcs_output_base_uri.rstrip('/')}/duplicate_audit"

        if final_parquet_stage_path.exists():
            shutil.rmtree(final_parquet_stage_path)
        if duplicate_audit_stage_path.exists():
            shutil.rmtree(duplicate_audit_stage_path)

        local_parquet_stage_dir.mkdir(parents=True, exist_ok=True)

        (
            final_docs.write.mode("overwrite")
            .parquet(str(final_parquet_stage_path))
        )

        duplicate_audit.write.mode("overwrite").parquet(
            str(duplicate_audit_stage_path)
        )

        final_upload_count = upload_directory_to_gcs(
            storage_client,
            final_parquet_stage_path,
            final_parquet_gcs_uri,
            clear_prefix=True,
        )
        duplicate_upload_count = upload_directory_to_gcs(
            storage_client,
            duplicate_audit_stage_path,
            duplicate_audit_gcs_uri,
            clear_prefix=True,
        )

        if not args.keep_local_parquet_stage and local_parquet_stage_dir.exists():
            shutil.rmtree(local_parquet_stage_dir)

        print("final_docs rows:", final_docs.count())
        print("uploaded final parquet files:", final_upload_count, "->", final_parquet_gcs_uri)
        print(
            "uploaded duplicate audit files:",
            duplicate_upload_count,
            "->",
            duplicate_audit_gcs_uri,
        )
        final_docs.orderBy("crawl_date", "source_file", "url").show(20, truncate=80)
    finally:
        spark.stop()


def main() -> None:
    args = parse_args()
    run_transform(args)


if __name__ == "__main__":
    main()
