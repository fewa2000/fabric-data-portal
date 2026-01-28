"""
Page 1 — Import
Inspect available import files and trigger a pipeline run.
"""

import io
import uuid

import pandas as pd
import streamlit as st

from config import get_app_version, get_fabric_config
from services import db, fabric_artifacts, fabric_pipelines, locking
from services.kpis import compute_import_profile

st.set_page_config(page_title="Import | Fabric Data Portal", layout="wide")
st.title("Import")
st.markdown("Inspect available import files and trigger a pipeline run.")

# ── Sidebar: user identity ─────────────────────────────────────
user_name = st.sidebar.text_input("Your name / identity", value="analyst")

# ── Lock status ─────────────────────────────────────────────────
try:
    locked, lock_info = locking.is_locked()
except Exception as e:
    st.error(f"Cannot check lock status: {e}")
    locked = True
    lock_info = None

if locked and lock_info:
    st.warning(
        f"A pipeline run is currently active "
        f"(run: `{lock_info.get('run_id')}`, "
        f"by: {lock_info.get('locked_by', 'unknown')}). "
        f"Please wait for it to finish before starting a new run."
    )

# ── Upload new file ─────────────────────────────────────────────
st.subheader("Upload Import File")
st.markdown("Upload a new file to `Files/import/` in the Lakehouse.")

uploaded_file = st.file_uploader(
    "Choose a file to upload",
    type=["xlsx", "xls", "csv"],
    help="Supported formats: Excel (.xlsx, .xls) and CSV (.csv)",
)

if uploaded_file is not None:
    # Show file info
    st.markdown(f"**Selected file:** `{uploaded_file.name}` ({uploaded_file.size:,} bytes)")

    if st.button("Upload to OneLake", type="primary"):
        with st.spinner(f"Uploading `{uploaded_file.name}` to OneLake..."):
            file_content = uploaded_file.read()
            success = fabric_artifacts.upload_import_file(uploaded_file.name, file_content)

        if success:
            st.success(f"File `{uploaded_file.name}` uploaded successfully.")
            # Clear session state to force refresh of file list
            if "import_profile" in st.session_state:
                del st.session_state["import_profile"]
            if "import_profile_file" in st.session_state:
                del st.session_state["import_profile_file"]
            st.rerun()
        else:
            st.error(
                f"Failed to upload `{uploaded_file.name}`. "
                "Check logs for details."
            )

st.divider()

# ── List import files ───────────────────────────────────────────
st.subheader("Available Import Files")

with st.spinner("Loading import files from OneLake..."):
    try:
        import_files = fabric_artifacts.list_import_files()
    except Exception as e:
        st.error(f"Failed to list import files: {e}")
        import_files = []

if not import_files:
    st.info(
        "No import files found. Ensure files exist in "
        "`Files/import/` in the Lakehouse."
    )
    st.stop()

file_names = [f["name"] for f in import_files]
selected_name = st.selectbox("Select an import file", file_names)
selected_file = next((f for f in import_files if f["name"] == selected_name), None)

# ── File metadata ───────────────────────────────────────────────
if selected_file:
    st.subheader("File Metadata")
    with st.container(border=True):
        c1, c2, c3 = st.columns(3)
        c1.markdown(f"**File Name**<br>`{selected_file['name']}`", unsafe_allow_html=True)
        c2.markdown(f"**Size**<br>{selected_file.get('size', 0):,} bytes", unsafe_allow_html=True)
        c3.markdown(f"**Last Modified**<br>{selected_file.get('last_modified', 'N/A')}", unsafe_allow_html=True)

# ── Import profile (on-demand) ──────────────────────────────────
st.subheader("Import Profile")

if st.button("Inspect File", type="secondary", disabled=not selected_file):
    with st.spinner("Downloading and analysing file..."):
        raw_bytes = fabric_artifacts.download_import_file(selected_name)
    if raw_bytes is None:
        st.error(f"Could not download `{selected_name}` from OneLake.")
    else:
        try:
            # Support both Excel and CSV files
            if selected_name.lower().endswith(".csv"):
                df_raw = pd.read_csv(io.BytesIO(raw_bytes))
            else:
                df_raw = pd.read_excel(io.BytesIO(raw_bytes))
            # Normalize columns (same logic as the Fabric notebook)
            df_raw.columns = [c.strip().lower().replace(" ", "_") for c in df_raw.columns]
            profile = compute_import_profile(
                df_raw,
                file_name=selected_name,
                file_size=selected_file.get("size"),
                last_modified=selected_file.get("last_modified"),
            )
            st.session_state["import_profile"] = profile
            st.session_state["import_profile_file"] = selected_name
        except Exception as e:
            st.error(f"Failed to read file: {e}")

# Display profile if available in session state
profile = st.session_state.get("import_profile")
profile_file = st.session_state.get("import_profile_file")

if profile and profile_file == selected_name:
    with st.container(border=True):
        pc1, pc2, pc3 = st.columns(3)
        pc1.markdown(f"**Row Count**<br>{profile.get('row_count', 'N/A'):,}", unsafe_allow_html=True)
        pc2.markdown(f"**Column Count**<br>{profile.get('column_count', 'N/A')}", unsafe_allow_html=True)
        date_range = profile.get("date_range")
        if date_range:
            pc3.markdown(
                f"**Date Range**<br>{date_range.get('min', '?')} to {date_range.get('max', '?')}",
                unsafe_allow_html=True,
            )

    # Schema preview
    schema = profile.get("schema", [])
    if schema:
        st.markdown("**Schema Preview**")
        schema_df = pd.DataFrame(schema)
        st.dataframe(schema_df, width='stretch', hide_index=True)

    # Validation summary
    validations = profile.get("validations", [])
    if validations:
        st.markdown("**Validation Results**")
        for v in validations:
            icon = "+" if v.get("passed") else "x"
            st.markdown(
                f"- [{icon}] **{v.get('check')}**: {v.get('detail')}"
            )

    # Sample rows
    sample_rows = profile.get("sample_rows", [])
    if sample_rows:
        st.markdown("**Sample Data (first 10 rows)**")
        st.dataframe(
            pd.DataFrame(sample_rows), width='stretch', hide_index=True
        )
else:
    st.info("Select a file and click **Inspect File** to view its profile.")

# ── Run Pipeline action ────────────────────────────────────────
st.subheader("Run Pipeline")

run_disabled = locked or not selected_file or not user_name.strip()
if not user_name.strip():
    st.caption("Enter your name in the sidebar to enable the Run button.")

# Custom styled centered button
st.markdown(
    """
    <style>
    div[data-testid="stButton"].run-pipeline-btn {
        display: flex;
        justify-content: center;
    }
    div[data-testid="stButton"].run-pipeline-btn > button {
        background-color: #3a3a3a;
        color: #ffffff;
        border: 1px solid #6b6b6b;
        border-radius: 6px;
        padding: 0.4rem 1.5rem;
        font-weight: 600;
        width: auto !important;
    }
    div[data-testid="stButton"].run-pipeline-btn > button:hover {
        background-color: #4a4a4a;
        border-color: #888888;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

_run_clicked = st.button(
    "Run Pipeline",
    disabled=run_disabled,
    key="run_pipeline_btn",
    width='stretch',
)

if _run_clicked:
    run_id = uuid.uuid4()
    cfg = get_fabric_config()

    # Acquire lock
    acquired = locking.acquire_lock(run_id, user_name)
    if not acquired:
        st.error(
            "Could not acquire pipeline lock. "
            "Another run may have started. Please refresh."
        )
        st.stop()

    try:
        # Trigger the pipeline
        with st.spinner("Triggering pipeline..."):
            result = fabric_pipelines.trigger_pipeline(
                input_file=selected_name,
                run_id=run_id,
                requested_by=user_name,
            )

        location_url = result["location_url"]

        # Log to Postgres
        try:
            db.insert_pipeline_run(
                run_id=run_id,
                triggered_by=user_name,
                input_file=selected_name,
                workspace_id=cfg.workspace_id,
                pipeline_item_id=cfg.pipeline_id,
                fabric_job_location_url=location_url,
                status="SUBMITTED",
                app_version=get_app_version(),
            )
            db.append_event(
                run_id=run_id,
                event_type="STATUS_CHANGE",
                message=f"Pipeline submitted by {user_name} for file {selected_name}",
            )
        except Exception as db_err:
            st.warning(
                f"Pipeline was triggered but failed to log to database: {db_err}. "
                f"The pipeline is running — check Monitor page."
            )

        st.success(
            f"Pipeline triggered successfully.\n\n"
            f"**Run ID:** `{run_id}`\n\n"
            f"Go to the **Monitor** page to track progress."
        )
        st.balloons()

    except Exception as e:
        # Release lock on failure
        try:
            locking.release_lock(run_id)
        except Exception:
            pass  # Best-effort lock release
        st.error(f"Pipeline trigger failed: {e}")
