"""
Fabric Data Portal — Main entry point.
Run with: streamlit run app.py
"""

import streamlit as st

st.set_page_config(
    page_title="Fabric Data Portal",
    page_icon=":",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Fabric Data Portal")
st.markdown(
    """
    Welcome to the **Fabric Data Portal**. Use the sidebar to navigate:

    - **Import** — Inspect import files and trigger pipeline runs
    - **Monitor** — Track active pipeline execution
    - **Results** — View latest KPIs and cleaned data
    - **Archive** — Browse run history and restore past results
    """
)

# Sidebar branding
st.sidebar.markdown("### Fabric Data Portal")
st.sidebar.markdown("---")

# Quick status check
try:
    from services.locking import is_locked
    locked, lock_info = is_locked()
    if locked:
        st.sidebar.warning(
            f"Pipeline running (by {lock_info.get('locked_by', 'unknown')})"
        )
    else:
        st.sidebar.success("No active pipeline run")
except Exception as e:
    st.sidebar.error(f"DB connection issue: {e}")
