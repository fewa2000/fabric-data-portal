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

        # Download buttons in a row
        col1, col2, col3 = st.columns(3)

        with col1:
            # Preview CSV (first 100 rows)
            preview_csv = df_preview.to_csv(index=False)
            st.download_button(
                label="Download CSV preview",
                data=preview_csv,
                file_name="sales_cleaned_preview.csv",
                mime="text/csv",
            )

        with col2:
            # Full CSV download
            st.download_button(
                label="Download full CSV",
                data=csv_text,
                file_name="sales_cleaned.csv",
                mime="text/csv",
            )

        with col3:
            # Full Parquet download
            try:
                parquet_data = fabric_artifacts.get_current_parquet()
                if parquet_data:
                    st.download_button(
                        label="Download full Parquet",
                        data=parquet_data,
                        file_name="sales_cleaned.parquet",
                        mime="application/octet-stream",
                    )
                else:
                    st.button("Download full Parquet", disabled=True, help="Parquet file not available")
            except Exception as e:
                st.button("Download full Parquet", disabled=True, help=f"Error: {e}")
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

        # Conversion funnel (only shown if visitor data exists in the source file)
        funnel = kpis.get("funnel")
        if funnel and isinstance(funnel, dict):
            st.markdown("---")
            st.markdown("**Conversion Funnel** *(derived from data)*")

            # Dynamic column layout based on available metrics
            funnel_metrics = []
            if "total_visitors" in funnel:
                funnel_metrics.append(("Total Visitors", f"{funnel['total_visitors']:,}"))
            if "converting_visitors" in funnel:
                funnel_metrics.append(("Converting Visitors", f"{funnel['converting_visitors']:,}"))
            funnel_metrics.append(("Orders", f"{funnel.get('orders', 0):,}"))
            if "conversion_rate_pct" in funnel:
                funnel_metrics.append(("Conversion Rate", f"{funnel['conversion_rate_pct']}%"))
            if "visitor_conversion_rate_pct" in funnel:
                funnel_metrics.append(("Visitor Conv. Rate", f"{funnel['visitor_conversion_rate_pct']}%"))

            fcols = st.columns(len(funnel_metrics))
            for i, (label, value) in enumerate(funnel_metrics):
                fcols[i].metric(label, value)

            if "definition" in funnel:
                st.caption(f"Definition: {funnel['definition']}")

        st.markdown("---")

        # Dynamic breakdowns - display all available revenue breakdowns
        # First check for legacy format (revenue_by_channel, etc. at top level)
        legacy_breakdowns = {}
        for key in ["revenue_by_channel", "revenue_by_region", "revenue_by_product_category"]:
            if key in kpis and isinstance(kpis[key], dict):
                legacy_breakdowns[key] = kpis[key]

        # Then check for new dynamic breakdowns format
        dynamic_breakdowns = kpis.get("breakdowns", {})
        if isinstance(dynamic_breakdowns, dict):
            # Merge, preferring dynamic over legacy for same keys
            all_breakdowns = {**legacy_breakdowns, **dynamic_breakdowns}
        else:
            all_breakdowns = legacy_breakdowns

        if all_breakdowns:
            st.markdown("**Revenue Breakdowns** *(auto-detected from data)*")

            # Display breakdowns in rows of 3
            breakdown_items = list(all_breakdowns.items())
            for i in range(0, len(breakdown_items), 3):
                row_items = breakdown_items[i:i + 3]
                cols = st.columns(len(row_items))
                for j, (breakdown_name, breakdown_data) in enumerate(row_items):
                    with cols[j]:
                        # Create a readable title from the key
                        title = breakdown_name.replace("revenue_by_", "").replace("_", " ").title()
                        st.markdown(f"**By {title}**")
                        if isinstance(breakdown_data, dict):
                            df_bd = pd.DataFrame(
                                list(breakdown_data.items()), columns=[title, "Revenue"]
                            )
                            st.dataframe(df_bd, width='stretch', hide_index=True)

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
