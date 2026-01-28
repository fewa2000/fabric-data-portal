"""
Page 2 — Monitor
Track active pipeline execution, poll status, view event timeline.
"""

import streamlit as st

from services import db, fabric_artifacts, fabric_pipelines, locking

st.set_page_config(page_title="Monitor | Fabric Data Portal", layout="wide")
st.title("Monitor")

# ── Auto-refresh toggle ────────────────────────────────────────
auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)
if auto_refresh:
    st.sidebar.caption("Page will re-run every 30 seconds.")

# ── Resolve run to display ────────────────────────────────────
try:
    locked, lock_info = locking.is_locked()
except Exception as e:
    st.error(f"Cannot check lock status: {e}")
    locked = False
    lock_info = None

try:
    run = db.get_active_run()
    if run is None:
        runs = db.list_runs(limit=1)
        run = runs[0] if runs else None
except Exception as e:
    st.error(f"Failed to load run data from database: {e}")
    run = None

# ── No run ever recorded ──────────────────────────────────────
if run is None:
    st.markdown(
        '<div style="text-align:center;padding:3rem 0">'
        '<span style="font-size:2.5rem">&#x1f535;</span><br>'
        '<span style="font-size:1.3rem;font-weight:600">No pipeline run yet</span><br>'
        '<span style="color:gray">Go to <b>Import</b> to start one.</span>'
        "</div>",
        unsafe_allow_html=True,
    )
    st.stop()

run_id = run["run_id"]
status = run["status"]
location_url = run.get("fabric_job_location_url", "")

# ── Status line ───────────────────────────────────────────────
_STATUS_MAP = {
    "SUCCEEDED": ("\U0001f7e2", "Pipeline succeeded"),
    "FAILED":    ("\U0001f534", "Pipeline failed"),
    "RUNNING":   ("\U0001f7e1", "Pipeline running"),
    "QUEUED":    ("\U0001f7e1", "Pipeline queued"),
    "SUBMITTED": ("\U0001f7e1", "Pipeline submitted"),
}
icon, label = _STATUS_MAP.get(status, ("\U0001f535", status))

st.markdown(f"{icon} **{label}**")

if run.get("error_message"):
    st.error(f"**Error:** {run['error_message']}")

# ── Run summary card ──────────────────────────────────────────
with st.container(border=True):
    st.markdown("**Run Summary**")

    # Row 1: primary info
    c1, c2, c3 = st.columns(3)
    c1.markdown(f"**Input File**<br>`{run.get('input_file', 'N/A')}`", unsafe_allow_html=True)
    c2.markdown(f"**Triggered By**<br>{run.get('triggered_by', 'N/A')}", unsafe_allow_html=True)
    c3.markdown(f"**Status**<br>`{status}`", unsafe_allow_html=True)

    # Row 2: timing
    t1, t2, t3 = st.columns(3)
    t1.markdown(f"**Created**<br>{str(run.get('created_at', ''))[:19]}", unsafe_allow_html=True)
    t2.markdown(f"**Started**<br>{str(run.get('started_at', 'N/A'))[:19]}", unsafe_allow_html=True)
    t3.markdown(f"**Finished**<br>{str(run.get('finished_at', 'N/A'))[:19]}", unsafe_allow_html=True)

    # Row 3: technical IDs (de-emphasised)
    st.caption(f"Run ID: {run_id}")
    if location_url:
        st.caption(f"Fabric Job Location: {location_url}")
    if run.get("fabric_job_id"):
        st.caption(f"Fabric Job ID: {run['fabric_job_id']}")

# ── Poll action ───────────────────────────────────────────────
if status in ("SUBMITTED", "QUEUED", "RUNNING"):
    if st.button("Refresh Status", type="primary"):
        if not location_url:
            st.error("No Fabric job location URL available for polling.")
        else:
            with st.spinner("Polling Fabric API..."):
                try:
                    poll_result = fabric_pipelines.poll_job_status(location_url)
                    fabric_status = poll_result.get("status", "Unknown")
                    new_status = fabric_pipelines.map_fabric_status(fabric_status)

                    # Update DB if status changed
                    if new_status != status:
                        try:
                            db.update_run_status(
                                run_id=run_id,
                                status=new_status,
                                fabric_job_id=poll_result.get("raw", {}).get("id"),
                            )
                            db.append_event(
                                run_id=run_id,
                                event_type="STATUS_CHANGE",
                                message=f"Status changed: {status} -> {new_status} "
                                        f"(Fabric: {fabric_status})",
                            )
                        except Exception as db_err:
                            st.warning(f"Status changed but failed to update database: {db_err}")

                        # On success, fetch KPIs from OneLake and persist to DB
                        if new_status == "SUCCEEDED":
                            try:
                                kpis_data = fabric_artifacts.get_run_kpis(str(run_id))
                                if kpis_data is None:
                                    kpis_data = fabric_artifacts.get_current_kpis()
                                if kpis_data is not None:
                                    db.update_run_status(
                                        run_id=run_id,
                                        status=new_status,
                                        kpis=kpis_data,
                                    )
                            except Exception:
                                pass  # Best-effort KPI persistence

                        # Release lock on terminal states
                        if new_status in ("SUCCEEDED", "FAILED"):
                            try:
                                locking.release_lock(run_id)
                                db.append_event(
                                    run_id=run_id,
                                    event_type="LOG",
                                    message=f"Pipeline lock released. Final status: {new_status}",
                                )
                            except Exception as lock_err:
                                st.warning(f"Failed to release lock: {lock_err}")

                        st.success(f"Status updated: **{status}** -> **{new_status}**")
                    else:
                        st.info(f"Status unchanged: **{status}** (Fabric: {fabric_status})")

                    # Show raw poll data
                    with st.expander("Raw poll response"):
                        st.json(poll_result.get("raw", {}))

                except Exception as e:
                    st.error(f"Polling failed: {e}")
                    try:
                        db.append_event(
                            run_id=run_id,
                            event_type="ERROR",
                            message=f"Poll error: {e}",
                        )
                    except Exception:
                        pass  # Best-effort event logging

# ── Event timeline ────────────────────────────────────────────
st.markdown("---")
st.subheader("Event Timeline")

try:
    events = db.get_events(run_id)
except Exception as e:
    st.error(f"Failed to load events: {e}")
    events = []

if events:
    for ev in events:
        etype = ev.get("event_type", "LOG")
        etime = str(ev.get("event_time", ""))[:19]
        msg = ev.get("message", "")

        if etype == "ERROR":
            st.markdown(f"- :red[**{etime}**] `{etype}` — {msg}")
        elif etype == "WARNING":
            st.markdown(f"- :orange[**{etime}**] `{etype}` — {msg}")
        elif etype == "STATUS_CHANGE":
            st.markdown(f"- :blue[**{etime}**] `{etype}` — {msg}")
        else:
            st.markdown(f"- **{etime}** `{etype}` — {msg}")
else:
    st.info("No events recorded yet for this run.")

# ── Auto-refresh mechanism ────────────────────────────────────
if auto_refresh and status in ("SUBMITTED", "QUEUED", "RUNNING"):
    import time as _time
    _time.sleep(30)
    st.rerun()
