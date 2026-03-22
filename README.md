- [Data Source: Common Crawl](#data-source-common-crawl)
  - [What is Common Crawl?](#what-is-common-crawl)
  - [What Problem is Common Crawl trying to solve?](#what-problem-is-common-crawl-trying-to-solve)
  - [Types of files on Common Crawl](#types-of-files-on-common-crawl)
- [Project: Large Web-Scale Deduplication and Cleaning](#project-large-web-scale-deduplication-and-cleaning)
  - [Problem Statement](#problem-statement)
- [Requirements](#requirements)
  - [Functional Requirements](#functional-requirements)
- [High-Level Design](#high-level-design)
  - [Select File Type (WET File ✅)](#select-file-type-wet-file-)
- [Implementation](#implementation)
  - [1. Repository File Structure](#1-repository-file-structure)
  - [2. Google Cloud Setup](#2-google-cloud-setup)
    - [What is a GCP Project?](#what-is-a-gcp-project)
    - [What is a Service Account?](#what-is-a-service-account)
    - [Assign Permissions (IAM Roles)](#assign-permissions-iam-roles)
    - [Download JSON Service Account Key](#download-json-service-account-key)
  - [3. Terraform](#3-terraform)
    - [Setup Steps](#setup-steps)
  - [4. Data Ingestion to GCS](#4-data-ingestion-to-gcs)
    - [Setup Python Virtual Environment](#setup-python-virtual-environment)
    - [Run Python Script](#run-python-script)
  - [5. Airflow DAG](#5-airflow-dag)
    - [Airflow: Workflow Orchestrator](#airflow-workflow-orchestrator)
      - [Airflow with Docker Compose](#airflow-with-docker-compose)

# Data Source: Common Crawl

About: https://commoncrawl.org/ <br>
Data Source: https://commoncrawl.org/get-started

## What is Common Crawl?

- Common Crawl is a giant public dump of a large sample of the web.
- Think of it as a **free, petabyte-scale backup of the public internet**.
- It is run by a non-profit organization, it is free to use and Common Crawl says the corpus spans over 300 billion pages with 3-5 billion new pages added each month.
- It is not the whole internet and it does not archive every page of every site, it is a sampled subset of the web.

## What Problem is Common Crawl trying to solve?

- Scraping the entire internet requires millions of dollars in compute, bandwidth and storage.
- Common Crawl does the heavy lifting of crawling the web and standardizing the data.
- This allows startups, researchers and engineers to skip the scraping phase and jump straight into analyzing the web or training AI models.
  - E.g., Common Crawl is a massive portion of the training data used for models like GPT-4 and Gemini.

## Types of files on Common Crawl

- WARC files: contain the raw HTML content of the pages.
- WAT files: contain metadata about the pages, such as HTTP headers and links.
- WET files: contain the extracted text content of the pages, without HTML tags.

# Project: Large Web-Scale Deduplication and Cleaning

## Problem Statement

- Common Crawl contains **duplicate pages, templated boilerplate, and low-quality text** that **reduce the usefulness of downstream analytics and ML datasets**.
- With these duplicate pages:
  - storage is wasted
  - analytics become misleading
  - machine learning training data gets worse
  - duplicate content inflates counts and distorts results
- This is an actual problem:
  - For **Search Engines**, near duplicates waste crawl/index resources and hurt result quality.
  - For **Analytics/Data Engineering**, duplicate and boilerplate-heavy pages distort counts and lower data quality.
  - For **LLM/data curation pipelines**, FineWeb mentioned that Common Crawl text is **noisy, inconsistent and unsuitable for direct use** without refinement and filtering.

**References**: <br>
https://arxiv.org/pdf/2111.10864 <br>
https://huggingface.co/spaces/HuggingFaceFW/blogpost-fineweb-v1

# Requirements

## Functional Requirements

The web is full of junk:

- duplicated articles copied across different sites.
- mirrored pages.
- headers, footers, menus, cookie banners, ads
- low-value spam pages
- pages that look different in URL but contain almost the same text.

The project aims to solve the following questions:

- Which pages contain **real, unique content**?
- Which pages are **near-duplicates** of each other?
- Which pages are mostly **boilerplate or spam**?

# High-Level Design

## Select File Type (WET File ✅)

- For this project, **WET files** are the best choice because they contain the extracted text content of the pages, without HTML tags.
- The problem is text cleaning and near-duplicate detection, not raw HTML rendering or link analysis, so WET files are the most relevant and efficient to work with.
- WARC files would require additional parsing to extract text and remove HTML, which adds complexity and overhead.
- WAT files contain metadata but not the actual text content, so they are not sufficient for this project.
- Also, Common Crawl's own WET example for a Wikipedia page includes navigation/menu text in the output, which is exactly the boilerplate problem we want to clean.

# Implementation

## 1. Repository File Structure

```
├── airflow
├── dashboard
├── README.md
├── scripts
├── sql
└── terraform
```

- `airflow/`: contains Airflow DAGs and related code for orchestrating the data pipeline.
- `dashboard/`: contains code for the dashboard that visualizes the results of the deduplication and cleaning process.
- `scripts/`: contains utility scripts for data processing, deduplication, cleaning, and other tasks.
- `sql/`: contains SQL queries for analyzing the cleaned data and generating insights.
- `terraform/`: contains Terraform code for provisioning any necessary infrastructure, such as cloud resources for storage, compute, or the dashboard.

## 2. Google Cloud Setup

- Create Google Cloud Project and Service Account, click [here](./docs/gcp-project-and-service-account.md) for detailed instructions.

---

### What is a GCP Project?

- A **GCP Project** is like a container for all your Google Cloud resources. It helps you organize and manage your cloud assets, permissions, billing, and more.
- You can have multiple projects for different purposes (e.g., development, staging, production) or different teams.
- Each project has its own settings and resources, but they can also interact with each other if needed.

---

### What is a Service Account?

- A **Service Account** is a special type of account meant for **applications or machines**.
- It allows our data pipeline or code to prove its identity to Google Cloud without having to share your personal login credentials.
- The **Service Account Name** you choose simply acts as the internal ID for this "bot" user.

### Assign Permissions (IAM Roles)

By default, a new service account has zero power. You must explicitly grant it **Identity and Access Management (IAM)** roles so it can perform tasks.

- **Storage Admin**: Gives the account full control over Google Cloud Storage (GCS). This is necessary if your pipeline needs to create buckets or upload the raw Common Crawl files.
- **BigQuery Admin**: Grants full access to BigQuery. This allows the account to create datasets and load the transformed data into tables for analysis.

### Download JSON Service Account Key

The **JSON key** is a file containing a private cryptographic key.

- When Python script or Terraform files run, they reference this file to "sign in", proving to Google Cloud that they are authorized to access the resources.
- **Note**: Because this file provides full administrative access to your storage and databases, it should never be committed to a public GitHub repository.
- Thus, we add to `.gitignore` to ensure it is not accidentally uploaded.

---

## 3. Terraform

Terraform is used here to provision the Google Cloud resources for the pipeline, starting with the raw GCS bucket.

For this project, I use a US region, specifically `us-east1`. The reason is that Common Crawl data is hosted as an AWS Open Data set in an Amazon S3 bucket located in the `us-east-1` region. Keeping the pipeline in a nearby US region reduces unnecessary long-distance data movement and is a better fit for workloads that process large volumes of Common Crawl data in the cloud.

### Setup Steps

1. Place your downloaded service account JSON key inside `terraform/keys/`.
2. Open `terraform/variables.tf` and confirm:
   - `project_id` matches your GCP project ID
   - `region` is set to `us-east1`
   - `credentials_file` matches your JSON filename inside `terraform/keys/`
3. Move into the Terraform folder and initialize the provider:

```bash
cd terraform
terraform init
```

4. Preview the infrastructure changes:

```bash
terraform plan
```

5. Apply the configuration to create the resources:

```bash
terraform apply
```

<img src="./docs/diagrams/gcp-cloud-storage-raw-bucket.png" />

The Google provider reads the credentials file from `terraform/keys/`, so the JSON key does not need to be hardcoded anywhere else. The key is also ignored by `.gitignore`, so it will not be committed accidentally.

## 4. Data Ingestion to GCS

- To get started, we will download a **small subset** of Common Crawl data locally.
- Common Crawl data is massive. If we don't sample, costs explode, jobs become slow/unreliable and debugging becomes painful.
- We will be using `CC-MAIN-2026-08` which is a crawl archive in February 2026 that contains 2.1 billion pages with 100000 WET files and 5.96 TiB (compressed size) of data.

### Setup Python Virtual Environment

```bash
# Run these commands to create a virtual environment and install python dependencies in that environment.
# This keeps your project dependencies isolated and organized, we want to avoid installing packages globally on our computer.
cd scripts/
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Run Python Script

- The ingestion script `ingest_to_gcs.py` will download a sample of WET files from Common Crawl and upload them to the GCS bucket we created with Terraform.
- Make sure to update the `BUCKET_NAME` variable in the script to match the name of your GCS bucket.

```bash
python ingest_to_gcs.py

# Output:
# (venv) ➜  scripts git:(main) ✗ python ingest_to_gcs.py
# [get_sampled_paths] Downloading manifest from https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/wet.paths.gz...
# [get_sampled_paths] Manifest downloaded successfully with status code 200.
# [get_sampled_paths] Total WET files available: 100000. Sampling 10 files...
# [main] Processing file 1/10: crawl-data/CC-MAIN-2026-12/segments/1772687277833.29/wet/CC-MAIN-20260310115014-20260310145014-00032.warc.wet.gz
# [stream_to_gcs] Streaming from https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687277833.29/wet/CC-MAIN-20260310115014-20260310145014-00032.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260310115014-20260310145014-00032.warc.wet.gz...
# [stream_to_gcs] Successfully streamed https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687277833.29/wet/CC-MAIN-20260310115014-20260310145014-00032.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260310115014-20260310145014-00032.warc.wet.gz.
# [main] Processing file 2/10: crawl-data/CC-MAIN-2026-12/segments/1772687278340.68/wet/CC-MAIN-20260312215302-20260313005302-00152.warc.wet.gz
# [stream_to_gcs] Streaming from https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687278340.68/wet/CC-MAIN-20260312215302-20260313005302-00152.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260312215302-20260313005302-00152.warc.wet.gz...
# [stream_to_gcs] Successfully streamed https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687278340.68/wet/CC-MAIN-20260312215302-20260313005302-00152.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260312215302-20260313005302-00152.warc.wet.gz.
# [main] Processing file 3/10: crawl-data/CC-MAIN-2026-12/segments/1772687277776.8/wet/CC-MAIN-20260309232602-20260310022602-00646.warc.wet.gz
# [stream_to_gcs] Streaming from https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687277776.8/wet/CC-MAIN-20260309232602-20260310022602-00646.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260309232602-20260310022602-00646.warc.wet.gz...
# [stream_to_gcs] Successfully streamed https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687277776.8/wet/CC-MAIN-20260309232602-20260310022602-00646.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260309232602-20260310022602-00646.warc.wet.gz.
# [main] Processing file 4/10: crawl-data/CC-MAIN-2026-12/segments/1772687277888.98/wet/CC-MAIN-20260310180006-20260310210006-00509.warc.wet.gz
# [stream_to_gcs] Streaming from https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687277888.98/wet/CC-MAIN-20260310180006-20260310210006-00509.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260310180006-20260310210006-00509.warc.wet.gz...
# [stream_to_gcs] Successfully streamed https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687277888.98/wet/CC-MAIN-20260310180006-20260310210006-00509.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260310180006-20260310210006-00509.warc.wet.gz.
# [main] Processing file 5/10: crawl-data/CC-MAIN-2026-12/segments/1772687556992.89/wet/CC-MAIN-20260316200809-20260316230809-00545.warc.wet.gz
# [stream_to_gcs] Streaming from https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687556992.89/wet/CC-MAIN-20260316200809-20260316230809-00545.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260316200809-20260316230809-00545.warc.wet.gz...
# [stream_to_gcs] Successfully streamed https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687556992.89/wet/CC-MAIN-20260316200809-20260316230809-00545.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260316200809-20260316230809-00545.warc.wet.gz.
# [main] Processing file 6/10: crawl-data/CC-MAIN-2026-12/segments/1772687278094.24/wet/CC-MAIN-20260311212513-20260312002513-00583.warc.wet.gz
# [stream_to_gcs] Streaming from https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687278094.24/wet/CC-MAIN-20260311212513-20260312002513-00583.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260311212513-20260312002513-00583.warc.wet.gz...
# [stream_to_gcs] Successfully streamed https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687278094.24/wet/CC-MAIN-20260311212513-20260312002513-00583.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260311212513-20260312002513-00583.warc.wet.gz.
# [main] Processing file 7/10: crawl-data/CC-MAIN-2026-12/segments/1772687278922.74/wet/CC-MAIN-20260314125656-20260314155656-00905.warc.wet.gz
# [stream_to_gcs] Streaming from https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687278922.74/wet/CC-MAIN-20260314125656-20260314155656-00905.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260314125656-20260314155656-00905.warc.wet.gz...
# [stream_to_gcs] Successfully streamed https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687278922.74/wet/CC-MAIN-20260314125656-20260314155656-00905.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260314125656-20260314155656-00905.warc.wet.gz.
# [main] Processing file 8/10: crawl-data/CC-MAIN-2026-12/segments/1772687277619.37/wet/CC-MAIN-20260308063647-20260308093647-00752.warc.wet.gz
# [stream_to_gcs] Streaming from https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687277619.37/wet/CC-MAIN-20260308063647-20260308093647-00752.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260308063647-20260308093647-00752.warc.wet.gz...
# [stream_to_gcs] Successfully streamed https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687277619.37/wet/CC-MAIN-20260308063647-20260308093647-00752.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260308063647-20260308093647-00752.warc.wet.gz.
# [main] Processing file 9/10: crawl-data/CC-MAIN-2026-12/segments/1772687277429.10/wet/CC-MAIN-20260306134848-20260306164848-00382.warc.wet.gz
# [stream_to_gcs] Streaming from https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687277429.10/wet/CC-MAIN-20260306134848-20260306164848-00382.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260306134848-20260306164848-00382.warc.wet.gz...
# [stream_to_gcs] Successfully streamed https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687277429.10/wet/CC-MAIN-20260306134848-20260306164848-00382.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260306134848-20260306164848-00382.warc.wet.gz.
# [main] Processing file 10/10: crawl-data/CC-MAIN-2026-12/segments/1772687278922.74/wet/CC-MAIN-20260314125656-20260314155656-00261.warc.wet.gz
# [stream_to_gcs] Streaming from https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687278922.74/wet/CC-MAIN-20260314125656-20260314155656-00261.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260314125656-20260314155656-00261.warc.wet.gz...
# [stream_to_gcs] Successfully streamed https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-12/segments/1772687278922.74/wet/CC-MAIN-20260314125656-20260314155656-00261.warc.wet.gz to gs://ccdp-raw-common-crawl-deduplication/raw/CC-MAIN-2026-12/CC-MAIN-20260314125656-20260314155656-00261.warc.wet.gz.

# [main] All files processed successfully. ✅
# [main] Finished retrieving sampled paths.
```

---

- Verified on GCP Cloud Storage bucket that the script works.

<img src="./docs/diagrams/gcp-cloud-storage-ccdp-sample-objects.png" />

## 5. Airflow DAG

- Current Problem:
  - Running `ingest_to_gcs.py` manually is fine for testing, but for a production pipeline we want to automate and schedule it.
  - We have multiple hardcoded variables in the script such as the bucket name, the Common Crawl crawl archive to sample from, and the number of files to ingest.
  - We want to be able to run this ingestion process on a regular schedule (e.g., monthly) to keep our dataset up to date with the latest Common Crawl data.

### Airflow: Workflow Orchestrator

- Airflow is a **workflow orchestrator**.
  - It does **NOT** process data itself.
  - It **controls and schedules** the steps in your data pipeline.
  - Think of it as: "The system that decides _what runs, when and in what order_".
  - Basically we want to avoid writing a bunch of scripts with hardcoded variables and running them manually on our computer. This is error prone and not scalable.
- With Airflow, we define our pipeline as a **Directed Acyclic Graph (DAG)**. Airflow handles execution order, retries, scheduling (e.g., monthly), logging and monitoring.

---

#### Airflow with Docker Compose

1. Download the official Airflow Docker Compose file:

```bash
touch docker-compose.yaml # create empty file
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.8/docker-compose.yaml'
```

2. Create the expected folders and `.env`:

```bash
mkdir -p ./dags ./logs ./plugins ./scripts
echo "AIRFLOW_UID=$(id -u)" > .env
```

3. Initialize Airflow

- This initializes the metadata database and creates the default setup.
- The [official Airflow tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/pipeline.html) uses this exact initialization step.

```bash
# From project root:
docker-compose up airflow-init
```

4. Start Airflow

```bash
# Start docker containers in detached mode (runs in the background)
docker-compose up -d

# Open on browser
#   Username: airflow
#   Password: airflow
http://localhost:8080
```

---
