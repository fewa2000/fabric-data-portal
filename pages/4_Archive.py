"""
Page 4 — Archive / History
Browse past runs, view KPIs per run, restore past results to current.
"""

import uuid

import pandas as pd
import streamlit as st

from services import db, fabric_artifacts, fabric_pipelines, locking

st.set_page_config(page_title="Archive | Fabric Data Portal", layout="wide")
st.title("Archive / History")
st.markdown("Browse past pipeline runs, compare KPIs, and restore results.")

# ── Sidebar: user identity for restore ──────────────────────────
user_name = st.sidebar.text_input("Your name / identity", value="analyst", key="archive_user")

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

        # Funnel
        funnel = run_kpis.get("funnel")
        if funnel:
            f1, f2, _ = st.columns(3)
            f1.metric("Total Visitors", f"{funnel.get('total_visitors', 0):,}")
            f2.metric("Conversion Rate", f"{funnel.get('conversion_rate_pct', 'N/A')}%")

    # Breakdowns
    with st.expander("Revenue Breakdowns"):
        bcol1, bcol2, bcol3 = st.columns(3)
        rev_ch = run_kpis.get("revenue_by_channel")
        if rev_ch:
            with bcol1:
                st.markdown("**By Channel**")
                st.dataframe(
                    pd.DataFrame(list(rev_ch.items()), columns=["Channel", "Revenue"]),
                    hide_index=True,
                )
        rev_rg = run_kpis.get("revenue_by_region")
        if rev_rg:
            with bcol2:
                st.markdown("**By Region**")
                st.dataframe(
                    pd.DataFrame(list(rev_rg.items()), columns=["Region", "Revenue"]),
                    hide_index=True,
                )
        rev_cat = run_kpis.get("revenue_by_product_category")
        if rev_cat:
            with bcol3:
                st.markdown("**By Category**")
                st.dataframe(
                    pd.DataFrame(list(rev_cat.items()), columns=["Category", "Revenue"]),
                    hide_index=True,
                )
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

# ── Restore action ──────────────────────────────────────────────
st.subheader("Restore to Current")
st.markdown(
    "Promote this run's output to `Files/results/current/`. "
    "This will overwrite the current results."
)

restore_disabled = (
    selected_run["status"] != "SUCCEEDED"
    or not user_name.strip()
)

if selected_run["status"] != "SUCCEEDED":
    st.caption("Only successful runs can be restored.")

if st.button(
    f"Restore run {str(selected_run_id)[:8]}... to current",
    disabled=restore_disabled,
    type="primary",
):
    try:
        with st.spinner("Triggering restore pipeline..."):
            # Trigger a lightweight pipeline/notebook to copy
            # runs/{run_id}/* -> current/*
            # For now, trigger the pipeline with restore parameters
            restore_run_id = uuid.uuid4()

            result = fabric_pipelines.trigger_pipeline(
                input_file=f"__restore__{selected_run_id}",
                run_id=restore_run_id,
                requested_by=user_name,
            )

            # Log the restore action
            restore_id = db.insert_restore(
                restored_by=user_name,
                source_run_id=selected_run_id,
                target_run_id=restore_run_id,
            )

            db.append_event(
                run_id=selected_run_id,
                event_type="LOG",
                message=f"Restore triggered by {user_name}. "
                        f"Restore ID: {restore_id}",
            )

        st.success(
            f"Restore initiated. Source run `{str(selected_run_id)[:8]}...` "
            f"will be promoted to `results/current/`.\n\n"
            f"Restore audit ID: `{restore_id}`"
        )

    except Exception as e:
        st.error(f"Restore failed: {e}")

        # Fallback: log the restore intent even if pipeline fails
        try:
            restore_id = db.insert_restore(
                restored_by=user_name,
                source_run_id=selected_run_id,
            )
            db.append_event(
                run_id=selected_run_id,
                event_type="WARNING",
                message=f"Restore requested by {user_name} but pipeline "
                        f"trigger failed: {e}. Restore ID: {restore_id}",
            )
        except Exception:
            pass

# ── Restore history ─────────────────────────────────────────────
st.subheader("Restore History")

try:
    restores = db.list_restores(limit=20)
except Exception as e:
    st.error(f"Failed to load restore history: {e}")
    restores = []

if restores:
    restore_rows = []
    for rr in restores:
        restore_rows.append({
            "Restored At": str(rr.get("restored_at", ""))[:19],
            "Restored By": rr.get("restored_by", ""),
            "Source Run": str(rr.get("source_run_id", ""))[:8] + "...",
            "Target Run": str(rr.get("target_run_id", "N/A"))[:8] + "..."
            if rr.get("target_run_id")
            else "N/A",
        })
    st.dataframe(
        pd.DataFrame(restore_rows), width='stretch', hide_index=True
    )
else:
    st.info("No restore actions recorded.")
