"""
Page 4 — Archive / History
Browse past runs, view KPIs per run (read-only).
"""

import pandas as pd
import streamlit as st

from services import db, fabric_artifacts

st.set_page_config(page_title="Archive | Fabric Data Portal", layout="wide")
st.title("Archive / History")
st.markdown("Browse past pipeline runs and view historical KPIs (read-only).")

# ── List all runs ───────────────────────────────────────────────
st.subheader("Pipeline Run History")

try:
    runs = db.list_runs(limit=100)
except Exception as e:
    st.error(f"Failed to load pipeline runs from database: {e}")
    runs = []

if not runs:
    st.info("No pipeline runs found.")
    st.stop()

# Build summary table
run_rows = []
for r in runs:
    run_rows.append({
        "Run ID": str(r["run_id"])[:8] + "...",
        "Full Run ID": str(r["run_id"]),
        "Status": r["status"],
        "Triggered By": r.get("triggered_by", ""),
        "Input File": r.get("input_file", ""),
        "Created": str(r.get("created_at", ""))[:19],
        "Finished": str(r.get("finished_at", "N/A"))[:19],
    })

df_runs = pd.DataFrame(run_rows)
st.dataframe(
    df_runs[["Run ID", "Status", "Triggered By", "Input File", "Created", "Finished"]],
    width='stretch',
    hide_index=True,
)

# ── Select a run to inspect ─────────────────────────────────────
st.subheader("Run Details")

run_options = {f"Run-Id: {str(r['run_id'])[:8]}... ({r['status']})": r for r in runs}
selected_label = st.selectbox("Select a run", list(run_options.keys()))
selected_run = run_options[selected_label]
selected_run_id = selected_run["run_id"]

# Run info
with st.container(border=True):
    c1, c2, c3 = st.columns(3)
    c1.markdown(f"**Status**<br>`{selected_run['status']}`", unsafe_allow_html=True)
    c2.markdown(f"**Triggered By**<br>{selected_run.get('triggered_by', 'N/A')}", unsafe_allow_html=True)
    c3.markdown(f"**Input File**<br>`{selected_run.get('input_file', 'N/A')}`", unsafe_allow_html=True)

if selected_run.get("error_message"):
    st.error(f"Error: {selected_run['error_message']}")

# ── KPIs for selected run ───────────────────────────────────────
st.subheader("KPIs for Selected Run")

run_kpis = selected_run.get("kpis")

# Also try reading from OneLake artifact
if run_kpis is None:
    try:
        run_kpis = fabric_artifacts.get_run_kpis(str(selected_run_id))
    except Exception:
        run_kpis = None

if run_kpis:
    with st.container(border=True):
        k1, k2, k3 = st.columns(3)
        k1.metric("Total Revenue", f"${run_kpis.get('total_revenue', 0):,.2f}")
        k2.metric("Orders", f"{run_kpis.get('orders', 0):,}")
        k3.metric("AOV", f"${run_kpis.get('aov', 0):,.2f}")

        # Funnel (only if visitor data was in the source file)
        funnel = run_kpis.get("funnel")
        if funnel:
            funnel_metrics = []
            if "total_visitors" in funnel:
                funnel_metrics.append(("Total Visitors", f"{funnel['total_visitors']:,}"))
            if "conversion_rate_pct" in funnel:
                funnel_metrics.append(("Conversion Rate", f"{funnel['conversion_rate_pct']}%"))
            if funnel_metrics:
                fcols = st.columns(len(funnel_metrics) + 1)  # +1 for empty spacer
                for i, (label, value) in enumerate(funnel_metrics):
                    fcols[i].metric(label, value)

    # Breakdowns - support both legacy and dynamic formats
    with st.expander("Revenue Breakdowns"):
        # Collect all breakdowns from legacy and dynamic formats
        legacy_breakdowns = {}
        for key in ["revenue_by_channel", "revenue_by_region", "revenue_by_product_category"]:
            if key in run_kpis and isinstance(run_kpis[key], dict):
                legacy_breakdowns[key] = run_kpis[key]

        dynamic_breakdowns = run_kpis.get("breakdowns", {})
        if isinstance(dynamic_breakdowns, dict):
            all_breakdowns = {**legacy_breakdowns, **dynamic_breakdowns}
        else:
            all_breakdowns = legacy_breakdowns

        if all_breakdowns:
            breakdown_items = list(all_breakdowns.items())
            for i in range(0, len(breakdown_items), 3):
                row_items = breakdown_items[i:i + 3]
                cols = st.columns(len(row_items))
                for j, (key, data) in enumerate(row_items):
                    with cols[j]:
                        title = key.replace("revenue_by_", "").replace("_", " ").title()
                        st.markdown(f"**By {title}**")
                        if isinstance(data, dict):
                            st.dataframe(
                                pd.DataFrame(list(data.items()), columns=[title, "Revenue"]),
                                hide_index=True,
                            )
        else:
            st.info("No breakdown data available.")
else:
    st.info("No KPI data available for this run.")

# ── Artifacts for selected run (disabled until artifact registration is implemented) ──
# st.subheader("Run Artifacts")
#
# artifacts = db.get_artifacts(selected_run_id)
# if artifacts:
#     art_rows = []
#     for a in artifacts:
#         art_rows.append({
#             "Type": a["artifact_type"],
#             "Path": a["file_path"],
#             "Size": f"{a.get('file_size', 0) or 0:,} bytes",
#             "Created": str(a.get("created_at", ""))[:19],
#         })
#     st.dataframe(
#         pd.DataFrame(art_rows), width='stretch', hide_index=True
#     )
# else:
#     st.info("No artifacts registered for this run.")
#     # Show expected paths
#     st.caption(
#         f"Expected artifact path: "
#         f"`Files/results/runs/{selected_run_id}/`"
#     )

# ── Events for selected run ─────────────────────────────────────
with st.expander("Event Timeline"):
    try:
        events = db.get_events(selected_run_id)
    except Exception as e:
        st.error(f"Failed to load events: {e}")
        events = []
    if events:
        for ev in events:
            etime = str(ev.get("event_time", ""))[:19]
            etype = ev.get("event_type", "LOG")
            msg = ev.get("message", "")
            st.markdown(f"- **{etime}** `{etype}` — {msg}")
    else:
        st.info("No events for this run.")
