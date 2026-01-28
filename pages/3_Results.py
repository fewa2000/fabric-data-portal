"""
Page 3 — Results
Display latest KPIs and cleaned data from results/current/.
"""

import io

import pandas as pd
import streamlit as st

from services import db, fabric_artifacts

st.set_page_config(page_title="Results | Fabric Data Portal", layout="wide")
st.title("Results")
st.markdown("Latest pipeline output from `Files/results/current/`.")

# ── Artifact locations ──────────────────────────────────────────
st.subheader("Artifact Locations")
st.markdown(
    """
| Artifact | Path |
|----------|------|
| Cleaned Parquet | `Files/results/current/sales_cleaned.parquet` |
| Cleaned CSV | `Files/results/current/sales_cleaned.csv` |
| KPIs | `Files/results/current/kpis.json` |
| Import Profile | `Files/results/current/import_profile.json` |
"""
)

# ── Cleaned data preview ────────────────────────────────────────
st.subheader("Cleaned Data Preview")
st.caption("Source: Files/results/current/sales_cleaned.csv")

try:
    csv_text = fabric_artifacts.get_current_sample_csv()
except Exception as e:
    st.error(f"Failed to fetch CSV data: {e}")
    csv_text = None

if csv_text:
    try:
        df_preview = pd.read_csv(io.StringIO(csv_text), nrows=100)
        st.dataframe(df_preview, width='stretch', hide_index=True)
        st.caption(f"Showing first {len(df_preview)} rows")

        # Download button
        st.download_button(
            label="Download CSV preview",
            data=csv_text,
            file_name="sales_cleaned_preview.csv",
            mime="text/csv",
        )
    except Exception as e:
        st.error(f"Failed to parse CSV data: {e}")
else:
    st.info(
        "No cleaned data preview available. "
        "Run the pipeline to produce output files."
    )

# ── Key Performance Indicators ──────────────────────────────────
st.subheader("Key Performance Indicators")

try:
    kpis = fabric_artifacts.get_current_kpis()
except Exception as e:
    st.error(f"Failed to fetch KPIs from OneLake: {e}")
    kpis = None

# Fallback: try latest successful run in Postgres
if kpis is None:
    try:
        latest_run = db.get_latest_successful_run()
        if latest_run and latest_run.get("kpis"):
            kpis = latest_run["kpis"]
            st.caption("(KPIs loaded from database — latest successful run)")
    except Exception as e:
        st.error(f"Failed to load KPIs from database: {e}")

if kpis is None:
    st.info(
        "No KPI data available. Run the pipeline to generate results."
    )
else:
    try:
        # Core metrics row
        col1, col2, col3 = st.columns(3)
        col1.metric(
            "Total Revenue",
            f"${kpis.get('total_revenue', 0):,.2f}",
        )
        col2.metric("Orders", f"{kpis.get('orders', 0):,}")
        col3.metric(
            "Avg Order Value (AOV)",
            f"${kpis.get('aov', 0):,.2f}",
        )

        # Conversion funnel
        funnel = kpis.get("funnel")
        if funnel and isinstance(funnel, dict):
            st.markdown("---")
            st.markdown("**Conversion Funnel**")
            fcol1, fcol2, fcol3, fcol4 = st.columns(4)
            fcol1.metric("Total Visitors", f"{funnel.get('total_visitors', 0):,}")
            fcol2.metric(
                "Converting Visitors", f"{funnel.get('converting_visitors', 0):,}"
            )
            fcol3.metric("Orders", f"{funnel.get('orders', 0):,}")
            fcol4.metric(
                "Conversion Rate",
                f"{funnel.get('conversion_rate_pct', 'N/A')}%",
            )
            st.caption(f"Definition: {funnel.get('definition', '')}")

        st.markdown("---")

        # Breakdowns side by side
        bcol1, bcol2, bcol3 = st.columns(3)

        rev_channel = kpis.get("revenue_by_channel")
        if rev_channel and isinstance(rev_channel, dict):
            with bcol1:
                st.markdown("**Revenue by Channel**")
                df_ch = pd.DataFrame(
                    list(rev_channel.items()), columns=["Channel", "Revenue"]
                )
                st.dataframe(df_ch, width='stretch', hide_index=True)

        rev_region = kpis.get("revenue_by_region")
        if rev_region and isinstance(rev_region, dict):
            with bcol2:
                st.markdown("**Revenue by Region**")
                df_rg = pd.DataFrame(
                    list(rev_region.items()), columns=["Region", "Revenue"]
                )
                st.dataframe(df_rg, width='stretch', hide_index=True)

        rev_cat = kpis.get("revenue_by_product_category")
        if rev_cat and isinstance(rev_cat, dict):
            with bcol3:
                st.markdown("**Revenue by Product Category**")
                df_cat = pd.DataFrame(
                    list(rev_cat.items()), columns=["Category", "Revenue"]
                )
                st.dataframe(df_cat, width='stretch', hide_index=True)

        # Time series
        ts = kpis.get("time_series_monthly")
        if ts and isinstance(ts, list):
            st.markdown("---")
            st.markdown("**Monthly Revenue & Orders**")
            df_ts = pd.DataFrame(ts)
            if "month" in df_ts.columns and "revenue" in df_ts.columns:
                st.bar_chart(df_ts.set_index("month")["revenue"])
            st.dataframe(df_ts, width='stretch', hide_index=True)
    except Exception as e:
        st.error(f"Failed to render KPI data: {e}")
        with st.expander("Raw KPI data"):
            st.json(kpis)
