# Common Crawl Deduplication Dashboard

A local Streamlit dashboard for visualizing the results of the Common Crawl deduplication pipeline.

## Features

The dashboard provides two main visualizations:

1. **Top Domains by Document Count** - A bar chart showing the categorical distribution of deduplicated documents across the most common domains
2. **Documents Over Time** - A line chart showing the temporal distribution of documents by crawl date

Additionally, the dashboard displays summary statistics including:

- Total document count
- Number of unique domains
- Number of crawl dates
- Average text length

## Requirements

- Python 3.9+
- Access to the BigQuery table created by the pipeline
- Service account key with BigQuery read permissions

## Installation

1. Install the required dependencies:

```bash
cd dashboard
pip install -r requirements.txt
```

2. Ensure your service account key is available at:

```
../terraform/keys/common-crawl-deduplication-184aa125ef30.json
```

Or update the path in the dashboard sidebar.

## Running the Dashboard

From the `dashboard` directory:

```bash
streamlit run app.py
```

The dashboard will open in your browser at `http://localhost:8501`.

This dashboard is intended to run locally for project evaluation and demos.

## Configuration

You can configure the following through the sidebar:

- **GCP Project ID**: Your Google Cloud project
- **BigQuery Dataset**: The dataset containing your data (default: `common_crawl_dedup`)
- **BigQuery Table**: The table name (default: `final_docs`)
- **Service Account Key Path**: Path to your service account JSON key file
- **Number of top domains**: How many domains to display in the bar chart (5-30)

## Data Refresh

The dashboard caches query results for 10 minutes (600 seconds) to improve performance. To force a refresh, click the three-dot menu in the top right and select "Clear cache" or restart the dashboard.

## Troubleshooting

**Error: Service account key not found**

- Verify the path to your service account key file is correct
- Ensure the file has the correct permissions

**Error: Failed to initialize BigQuery client**

- Check that your service account has `BigQuery Data Viewer` or `BigQuery Admin` permissions
- Verify your GCP project ID is correct

**No data displayed**

- Ensure the pipeline has completed successfully and loaded data into BigQuery
- Verify the dataset and table names match your configuration
- Check that the table contains data by running a query in the BigQuery console
