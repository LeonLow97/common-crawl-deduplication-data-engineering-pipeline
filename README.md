- [Evaluation Checklist](#evaluation-checklist)
- [Data Source: Common Crawl](#data-source-common-crawl)
  - [What is Common Crawl?](#what-is-common-crawl)
  - [Why Common Crawl Exists](#why-common-crawl-exists)
  - [Types of Common Crawl Files](#types-of-common-crawl-files)
- [Project: Large Web-Scale Deduplication and Cleaning](#project-large-web-scale-deduplication-and-cleaning)
  - [Problem Statement](#problem-statement)
  - [What This Project Produces](#what-this-project-produces)
  - [Requirements](#requirements)
    - [Functional Requirements](#functional-requirements)
- [High-Level Design](#high-level-design)
  - [Why WET Files](#why-wet-files)
- [Repository Structure](#repository-structure)
- [Setup Guide](#setup-guide)
  - [References](#references)

# Evaluation Checklist

| Criterion                      | Status | Score | Notes                                                                                                                         |
| ------------------------------ | ------ | ----- | ----------------------------------------------------------------------------------------------------------------------------- |
| Problem description            | ✅     | 4/4   | The README clearly explains the noise, duplication, and boilerplate problem in Common Crawl and what this project is solving. |
| Cloud                          | ✅     | 4/4   | The project uses Google Cloud and provisions infrastructure with Terraform.                                                   |
| Batch / Workflow orchestration | ✅     | 4/4   | The Airflow DAG orchestrates ingestion to GCS, transformation, and loading into BigQuery end to end.                          |
| Data warehouse                 | ⚠️     | 2/4   | BigQuery is used, and the loader supports time partitioning on `crawl_date`, but clustering is not defined or documented yet. |
| Transformations                | ✅     | 4/4   | The transformation layer is implemented in Spark.                                                                             |
| Dashboard                      | ✅     | 4/4   | Local Streamlit dashboard with domain distribution (categorical) and temporal distribution tiles. See [dashboard/](./dashboard/). |
| Reproducibility                | ✅     | 4/4   | The setup instructions are clear, step by step, and documented in [docs/setup.md](./docs/setup.md).                           |

# Data Source: Common Crawl

About: https://commoncrawl.org/  
Getting Started: https://commoncrawl.org/get-started

## What is Common Crawl?

- Common Crawl is a large public dataset of web crawl data.
- You can think of it as a free, internet-scale snapshot of the public web.
- It is maintained by a non-profit organization and gives researchers, engineers, and companies access to web data without having to crawl the internet themselves.
- It is not the whole internet. It is a sampled and curated slice of the web.

## Why Common Crawl Exists

- Crawling the public web at scale is expensive and operationally difficult.
- Common Crawl solves that by collecting the data once and publishing it for others to use.
- That makes it much easier to build search, analytics, and machine learning workflows on top of web-scale text.

## Types of Common Crawl Files

- `WARC`: raw web responses, including HTML
- `WAT`: metadata such as headers and links
- `WET`: extracted plain text with HTML removed

# Project: Large Web-Scale Deduplication and Cleaning

## Problem Statement

Common Crawl is valuable, but it is also noisy.

A large crawl of the web contains:

- duplicate pages copied across domains
- mirrored or near-identical pages
- navigation menus, headers, footers, cookie banners, and other boilerplate
- low-value or spam-like content

That becomes a real data engineering problem because raw Common Crawl text is not automatically clean enough for analytics or downstream ML use.

Without cleaning and deduplication:

- storage is wasted on repeated content
- document counts become misleading
- downstream analytics are less trustworthy
- machine learning datasets become lower quality

This project addresses that problem by building a pipeline that ingests sampled Common Crawl WET files, transforms them into cleaner parquet outputs, and loads the final result into BigQuery for analysis.

## What This Project Produces

The pipeline is designed to help answer questions like:

- Which pages contain real, unique content?
- Which pages are near-duplicates of each other?
- Which pages are mostly boilerplate or low-value text?

## Requirements

### Functional Requirements

The project should be able to:

- ingest sampled Common Crawl WET files into Google Cloud Storage
- process the text into cleaner parquet outputs
- support deduplication-oriented analysis on the resulting dataset
- load the final outputs into BigQuery for querying and downstream work

# High-Level Design

## Why WET Files

- This project focuses on text cleaning and near-duplicate detection, so `WET` files are the best fit.
- `WET` files already contain extracted text, which removes a lot of the overhead of parsing raw HTML first.
- `WARC` files would include raw page content and require extra HTML extraction work.
- `WAT` files contain metadata, but not the actual text body we want to clean and compare.

# Repository Structure

```text
.
├── batch/
├── dags/
├── dashboard/
├── docs/
├── scripts/
├── terraform/
├── Dockerfile
└── docker-compose.yaml
```

- `batch/` contains the Spark-based transformation logic.
- `dags/` contains the Airflow DAGs that orchestrate the pipeline.
- `dashboard/` contains the Streamlit dashboard for visualizing pipeline results.
- `docs/` contains setup notes, diagrams, and runbook-style documentation.
- `scripts/` contains ingestion and BigQuery loading scripts.
- `terraform/` contains infrastructure code for provisioning Google Cloud resources.

# Setup Guide

For the full setup instructions, go to [docs/setup.md](./docs/setup.md).

That guide covers:

- creating the GCP project and service account
- placing the JSON key in `terraform/keys/`
- running `terraform apply`
- starting Airflow with Docker Compose
- logging into `http://localhost:8080`
- triggering `common_crawl_gcs_to_bigquery_dag`
- viewing results in the local Streamlit dashboard at `http://localhost:8501`

## References

- https://arxiv.org/pdf/2111.10864
- https://huggingface.co/spaces/HuggingFaceFW/blogpost-fineweb-v1
