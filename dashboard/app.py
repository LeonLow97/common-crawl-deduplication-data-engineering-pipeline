"""
Common Crawl Deduplication Dashboard

This dashboard visualizes the results of the deduplication pipeline with:
- Top domains by document count (categorical distribution)
- Documents over time by crawl date (temporal distribution)
"""

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account


# Configuration
DEFAULT_PROJECT_ID = "common-crawl-deduplication"
DEFAULT_DATASET_ID = "common_crawl_dedup"
DEFAULT_TABLE_ID = "final_docs"
DEFAULT_KEY_FILE = "../terraform/keys/common-crawl-deduplication-184aa125ef30.json"


def get_bigquery_client(key_file: str, project_id: str) -> bigquery.Client:
    """Create BigQuery client with service account credentials."""
    key_path = Path(key_file).expanduser().resolve()

    if not key_path.exists():
        st.error(f"Service account key not found at: {key_path}")
        st.stop()

    credentials = service_account.Credentials.from_service_account_file(str(key_path))
    return bigquery.Client(credentials=credentials, project=project_id)


@st.cache_data(ttl=600)
def query_top_domains(
    _client: bigquery.Client, dataset_id: str, table_id: str, limit: int = 15
) -> pd.DataFrame:
    """Query top domains by document count."""
    query = f"""
    SELECT
        domain,
        COUNT(*) as doc_count
    FROM `{_client.project}.{dataset_id}.{table_id}`
    WHERE domain IS NOT NULL
    GROUP BY domain
    ORDER BY doc_count DESC
    LIMIT {limit}
    """
    return _client.query(query).to_dataframe()


@st.cache_data(ttl=600)
def query_temporal_distribution(
    _client: bigquery.Client, dataset_id: str, table_id: str
) -> pd.DataFrame:
    """Query document counts by crawl date."""
    query = f"""
    SELECT
        crawl_date,
        COUNT(*) as doc_count
    FROM `{_client.project}.{dataset_id}.{table_id}`
    WHERE crawl_date IS NOT NULL
    GROUP BY crawl_date
    ORDER BY crawl_date
    """
    return _client.query(query).to_dataframe()


@st.cache_data(ttl=600)
def query_summary_stats(
    _client: bigquery.Client, dataset_id: str, table_id: str
) -> dict:
    """Query summary statistics."""
    query = f"""
    SELECT
        COUNT(*) as total_docs,
        COUNT(DISTINCT domain) as unique_domains,
        COUNT(DISTINCT crawl_date) as crawl_dates,
        AVG(text_len) as avg_text_length
    FROM `{_client.project}.{dataset_id}.{table_id}`
    """
    result = _client.query(query).to_dataframe()
    return result.iloc[0].to_dict()


def main():
    st.set_page_config(
        page_title="Common Crawl Deduplication Dashboard", page_icon="📊", layout="wide"
    )

    st.title("📊 Common Crawl Deduplication Dashboard")
    st.markdown("Visualizing deduplicated web crawl data from Common Crawl")

    # Sidebar configuration
    st.sidebar.header("Configuration")

    project_id = st.sidebar.text_input("GCP Project ID", value=DEFAULT_PROJECT_ID)
    dataset_id = st.sidebar.text_input("BigQuery Dataset", value=DEFAULT_DATASET_ID)
    table_id = st.sidebar.text_input("BigQuery Table", value=DEFAULT_TABLE_ID)
    key_file = st.sidebar.text_input("Service Account Key Path", value=DEFAULT_KEY_FILE)
    top_n_domains = st.sidebar.slider(
        "Number of top domains to show", min_value=5, max_value=30, value=15
    )

    # Initialize BigQuery client
    try:
        client = get_bigquery_client(key_file, project_id)
    except Exception as e:
        st.error(f"Failed to initialize BigQuery client: {e}")
        st.stop()

    # Query data
    try:
        with st.spinner("Loading data from BigQuery..."):
            summary_stats = query_summary_stats(client, dataset_id, table_id)
            top_domains_df = query_top_domains(
                client, dataset_id, table_id, top_n_domains
            )
            temporal_df = query_temporal_distribution(client, dataset_id, table_id)

        # Display summary metrics
        st.header("📈 Summary Statistics")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Documents", f"{summary_stats['total_docs']:,}")
        with col2:
            st.metric("Unique Domains", f"{summary_stats['unique_domains']:,}")
        with col3:
            st.metric("Crawl Dates", f"{summary_stats['crawl_dates']:,}")
        with col4:
            st.metric(
                "Avg Text Length", f"{summary_stats['avg_text_length']:.0f} chars"
            )

        st.divider()

        # Tile 1: Top Domains (Categorical Distribution)
        st.header("🌐 Top Domains by Document Count")
        st.markdown(
            "Distribution of deduplicated documents across the most common domains"
        )

        if not top_domains_df.empty:
            fig_domains = px.bar(
                top_domains_df,
                x="domain",
                y="doc_count",
                title=f"Top {top_n_domains} Domains",
                labels={"domain": "Domain", "doc_count": "Document Count"},
                color="doc_count",
                color_continuous_scale="Blues",
            )
            fig_domains.update_layout(xaxis_tickangle=-45, height=500, showlegend=False)
            st.plotly_chart(fig_domains, use_container_width=True)

            # Show data table
            with st.expander("View Domain Data Table"):
                st.dataframe(top_domains_df, use_container_width=True, hide_index=True)
        else:
            st.warning("No domain data available")

        st.divider()

        # Tile 2: Temporal Distribution
        st.header("📅 Document Distribution Over Time")
        st.markdown("Number of deduplicated documents by crawl date")

        if not temporal_df.empty:
            fig_temporal = px.line(
                temporal_df,
                x="crawl_date",
                y="doc_count",
                title="Documents Over Time",
                labels={"crawl_date": "Crawl Date", "doc_count": "Document Count"},
                markers=True,
            )
            fig_temporal.update_layout(
                height=500, xaxis_title="Crawl Date", yaxis_title="Document Count"
            )
            fig_temporal.update_traces(line_color="#1f77b4", line_width=3)
            st.plotly_chart(fig_temporal, use_container_width=True)

            # Show data table
            with st.expander("View Temporal Data Table"):
                temporal_display = temporal_df.copy()
                temporal_display["crawl_date"] = temporal_display[
                    "crawl_date"
                ].dt.strftime("%Y-%m-%d")
                st.dataframe(
                    temporal_display, use_container_width=True, hide_index=True
                )
        else:
            st.warning("No temporal data available")

        # Footer
        st.divider()
        st.markdown("### About This Dashboard")
        st.markdown(
            """
        This dashboard visualizes data from the Common Crawl deduplication pipeline:
        - **Top Domains**: Shows which domains contribute the most unique content after deduplication
        - **Temporal Distribution**: Shows how documents are distributed across crawl dates
        - Data is refreshed every 10 minutes
        """
        )

    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
