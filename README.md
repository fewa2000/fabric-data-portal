  ---
  Fabric Data Portal — Technical Architecture Deep-Dive

  System Overview

  ┌─────────────────────────────────────────────────────────────────────┐
  │                        STREAMLIT APPLICATION                        │
  │                                                                     │
  │  Home.py ─── pages/1_Import.py ─── pages/2_Monitor.py              │
  │              pages/3_Results.py ─── pages/4_Archive.py              │
  ├─────────────────────────────────────────────────────────────────────┤
  │                         SERVICE LAYER                               │
  │                                                                     │
  │  config.py        services/db.py           services/locking.py      │
  │  services/fabric_auth.py    services/fabric_pipelines.py            │
  │  services/fabric_artifacts.py              services/kpis.py         │
  ├───────────────────────┬─────────────────────────────────────────────┤
  │   POSTGRESQL          │        MICROSOFT FABRIC                     │
  │                       │                                             │
  │  pipeline_runs        │  OneLake DFS (artifact storage)             │
  │  run_lock             │  Fabric Jobs API (pipeline trigger/poll)    │
  │  run_events           │  Entra ID OAuth (authentication)            │
  │  run_artifacts        │  Fabric Notebook (execution engine)         │
  │  run_restores         │                                             │
  └───────────────────────┴─────────────────────────────────────────────┘

  ---
  Module Dependency Graph

  config.py
   ├──► services/fabric_auth.py
   │      ├──► services/fabric_pipelines.py
   │      └──► services/fabric_artifacts.py
   └──► services/db.py
          └──► services/locking.py

  services/kpis.py  (standalone, no service dependencies)

  pages/1_Import.py  ──► config, db, locking, fabric_artifacts, fabric_pipelines, kpis
  pages/2_Monitor.py ──► db, locking, fabric_artifacts, fabric_pipelines
  pages/3_Results.py ──► db, fabric_artifacts
  pages/4_Archive.py ──► db, fabric_artifacts, fabric_pipelines, locking
  Home.py            ──► locking

  ---
  Foundation Layer

  config.py — Configuration Loader
  Column 1: Input
  Detail: .streamlit/secrets.toml (primary), environment variables (fallback)
  ────────────────────────────────────────
  Column 1: Process
  Detail: _get(key) checks Streamlit secrets first, falls back to os.getenv(). Two frozen dataclasses (FabricConfig,
    PgConfig) provide typed access. Port conversion protected with int() fallback to 5432.
  ────────────────────────────────────────
  Column 1: Output
  Detail: FabricConfig (8 fields: tenant_id, client_id, client_secret, workspace_id, pipeline_id, lakehouse_id,
    owner_upn, owner_object_id), PgConfig (5 fields: host, port, database, user, password), app_version string
  ────────────────────────────────────────
  Column 1: Consumers
  Detail: Every service module
  services/db.py — PostgreSQL Connection Pool & Query Helpers
  Column 1: Input
  Detail: PgConfig from config.py
  ────────────────────────────────────────
  Column 1: Process
  Detail: Lazy-initialized ThreadedConnectionPool (1–5 connections). Two context managers: get_conn() borrows/returns
    connections, get_cursor(commit=True) provides RealDictCursor with auto-commit/rollback.
  ────────────────────────────────────────
  Column 1: Output
  Detail: Query results as list[dict] or `dict
  Function inventory and table mapping:
  Function: insert_pipeline_run()
  Table: pipeline_runs
  Operation: INSERT
  Called by: Import page
  ────────────────────────────────────────
  Function: update_run_status()
  Table: pipeline_runs
  Operation: UPDATE (status, timestamps, error_message, fabric_job_id, kpis)
  Called by: Monitor page
  ────────────────────────────────────────
  Function: get_run()
  Table: pipeline_runs
  Operation: SELECT by run_id
  Called by: (available, unused currently)
  ────────────────────────────────────────
  Function: get_active_run()
  Table: pipeline_runs
  Operation: SELECT WHERE status IN (SUBMITTED, QUEUED, RUNNING)
  Called by: Monitor page
  ────────────────────────────────────────
  Function: get_latest_successful_run()
  Table: pipeline_runs
  Operation: SELECT WHERE status = SUCCEEDED, latest
  Called by: Results page
  ────────────────────────────────────────
  Function: list_runs()
  Table: pipeline_runs
  Operation: SELECT ORDER BY created_at DESC LIMIT n
  Called by: Monitor page, Archive page
  ────────────────────────────────────────
  Function: append_event()
  Table: run_events
  Operation: INSERT
  Called by: Import, Monitor, Archive pages
  ────────────────────────────────────────
  Function: get_events()
  Table: run_events
  Operation: SELECT by run_id ORDER BY event_time
  Called by: Monitor page, Archive page
  ────────────────────────────────────────
  Function: insert_artifact()
  Table: run_artifacts
  Operation: INSERT
  Called by: (available, not yet wired)
  ────────────────────────────────────────
  Function: get_artifacts()
  Table: run_artifacts
  Operation: SELECT by run_id
  Called by: (available, commented out in Archive)
  ────────────────────────────────────────
  Function: insert_restore()
  Table: run_restores
  Operation: INSERT, returns restore_id
  Called by: Archive page
  ────────────────────────────────────────
  Function: list_restores()
  Table: run_restores
  Operation: SELECT ORDER BY restored_at DESC LIMIT n
  Called by: Archive page
  update_run_status() dynamic SET construction:

  Always sets:    status
  If RUNNING:     + started_at = now()
  If SUCCEEDED|FAILED: + finished_at = now()
  If error_message:    + error_message
  If fabric_job_id:    + fabric_job_id
  If kpis:             + kpis (JSON serialized with default=str fallback)

  services/locking.py — Concurrency Lock
  Column 1: Input
  Detail: run_lock table (single-row, fixed key ACTIVE_PIPELINE_RUN)
  ────────────────────────────────────────
  Column 1: Process
  Detail: Direct pool access via _get_pool(). Each function borrows a connection, executes raw SQL, commits/rollbacks,
    returns connection.
  ────────────────────────────────────────
  Column 1: Output
  Detail: Boolean results + side-effect on run_lock row
  State machine for run_lock:

  ┌─────────────┐    acquire_lock(run_id, user)     ┌──────────────┐
  │  UNLOCKED   │ ─────────────────────────────────► │   LOCKED     │
  │  run_id=NULL│   UPDATE WHERE run_id IS NULL      │  run_id=UUID │
  │             │   (atomic, returns rowcount=1)      │  locked_by=  │
  └─────────────┘                                    │  locked_at=  │
        ▲                                            └──────┬───────┘
        │         release_lock(run_id)                      │
        └───────────────────────────────────────────────────┘
                  UPDATE WHERE run_id = %s
                  SET run_id=NULL, locked_at=NULL, locked_by=NULL

  force_release_lock() unconditionally NULLs all fields (admin recovery).

  Multi-user safety: The UPDATE ... WHERE run_id IS NULL pattern is atomic at the database level. If two users race,
  only one UPDATE affects a row (rowcount=1), the other gets rowcount=0 and returns False.

  ---
  Authentication Layer

  services/fabric_auth.py — OAuth Client-Credentials Token Manager
  Column 1: Input
  Detail: FabricConfig (tenant_id, client_id, client_secret)
  ────────────────────────────────────────
  Column 1: Process
  Detail: POST to https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token with grant_type=client_credentials.
    Per-scope caching in _token_cache: dict[str, TokenInfo]. Refresh trigger: within 300s of expires_at.
  ────────────────────────────────────────
  Column 1: Output
  Detail: Bearer token string, wrapped as Authorization headers by convenience functions
  Two independent token scopes:
  Function: get_access_token() → get_auth_headers()
  Scope: https://api.fabric.microsoft.com/.default
  Used for: Pipeline trigger, job polling
  ────────────────────────────────────────
  Function: get_storage_token() → get_storage_headers()
  Scope: https://storage.azure.com/.default
  Used for: OneLake DFS file reads
  Cache lifecycle:
  Request token → Check _token_cache[scope]
    ├─ Hit + (now < expires_at - 300s) → return cached
    └─ Miss or expired → POST to Entra ID → cache TokenInfo → return

  ---
  Fabric Integration Layer

  services/fabric_pipelines.py — Pipeline Trigger & Job Polling
  Column 1: Input
  Detail: input_file, run_id (UUID), requested_by (user string)
  ────────────────────────────────────────
  Column 1: Process
  Detail: Constructs payload with executionData containing OwnerUserPrincipalName, OwnerUserObjectId, and pipeline
    parameters. POST to Fabric Jobs API.
  ────────────────────────────────────────
  Column 1: Output
  Detail: location_url for polling, status strings
  trigger_pipeline() IPO:

  Input:   input_file, run_id, requested_by
  Process: POST /v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline
           Body: { executionData: { OwnerUPN, OwnerObjectId, parameters: {input_file, run_id, requested_by} } }
           Expect: HTTP 202
  Output:  { status_code: 202, location_url: "https://...", response_headers: {...} }

  poll_job_status() IPO:

  Input:   location_url (from trigger response Location header)
  Process: GET {location_url} with Fabric auth headers
           HTTP 200 → terminal state, parse JSON body
           HTTP 202 → still running, try parse JSON body
           Other    → PollError
  Output:  { status: "InProgress"|"Completed"|"Failed"|..., raw: {...}, http_status: int }

  map_fabric_status() translation table:

  Fabric Status    →  pipeline_runs.status
  ─────────────────────────────────────────
  NotStarted       →  QUEUED
  InProgress        →  RUNNING
  Completed         →  SUCCEEDED
  Failed            →  FAILED
  Cancelled         →  FAILED
  Deduped           →  FAILED
  (unknown)         →  RUNNING (default)

  services/fabric_artifacts.py — OneLake DFS File Reader
  Column 1: Input
  Detail: Relative file paths within lakehouse (e.g., Files/results/current/kpis.json)
  ────────────────────────────────────────
  Column 1: Process
  Detail: Builds URL: https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}/{path}. GET with
    storage-scope auth. Returns parsed JSON, raw CSV text, raw bytes, or structured file listings.
  ────────────────────────────────────────
  Column 1: Output
  Detail: `dict
  Function inventory:
  Function: list_import_files()
  OneLake Path: Files/import?resource=filesystem
  Return Type: list[dict] with name, size, last_modified
  Used by: Import page
  ────────────────────────────────────────
  Function: download_import_file(name)
  OneLake Path: Files/import/{name}
  Return Type: `bytes
  Used by: None`
  ────────────────────────────────────────
  Function: get_current_kpis()
  OneLake Path: Files/results/current/kpis.json
  Return Type: `dict
  Used by: None`
  ────────────────────────────────────────
  Function: get_current_import_profile()
  OneLake Path: Files/results/current/import_profile.json
  Return Type: `dict
  Used by: None`
  ────────────────────────────────────────
  Function: get_current_sample_csv()
  OneLake Path: Files/results/current/sales_cleaned.csv
  Return Type: `str
  Used by: None`
  ────────────────────────────────────────
  Function: get_run_kpis(run_id)
  OneLake Path: Files/results/runs/{run_id}/kpis.json
  Return Type: `dict
  Used by: None`
  ────────────────────────────────────────
  Function: get_run_import_profile(run_id)
  OneLake Path: Files/results/runs/{run_id}/import_profile.json
  Return Type: `dict
  Used by: None`
  ────────────────────────────────────────
  Function: get_run_metadata(run_id)
  OneLake Path: Files/results/runs/{run_id}/run_metadata.json
  Return Type: `dict
  Used by: None`
  All functions return None on any HTTP error or exception (graceful degradation).

  services/kpis.py — KPI Computation Engine
  Column 1: Input
  Detail: Cleaned pd.DataFrame with normalized column names
  ────────────────────────────────────────
  Column 1: Process
  Detail: Validates required columns (revenue, order_id). Computes core metrics, conversion funnel, breakdowns by
    channel/region/product_category, monthly time series. Each breakdown is independently guarded.
  ────────────────────────────────────────
  Column 1: Output
  Detail: dict[str, Any] matching the kpis.json schema
  compute_kpis() output schema:

  {
    "total_revenue": float,
    "orders": int,
    "aov": float,
    "funnel": {
      "total_visitors": 1428571,
      "converting_visitors": 49080,
      "orders": int,
      "conversion_rate_pct": float,
      "definition": "conversion_rate = orders / total_visitors"
    },
    "revenue_by_channel": { "channel_name": float, ... },
    "revenue_by_region": { "region_name": float, ... },
    "revenue_by_product_category": { "category_name": float, ... },
    "time_series_monthly": [ { "month": "YYYY-MM", "revenue": float, "orders": int }, ... ]
  }

  compute_import_profile() output schema:

  {
    "file_name": str,
    "file_size": int | null,
    "last_modified": str | null,
    "row_count": int,
    "column_count": int,
    "schema": [ { "column": str, "dtype": str, "null_count": int, "null_pct": float } ],
    "date_range": { "min": str, "max": str },
    "validations": [ { "check": str, "passed": bool, "detail": str } ],
    "sample_rows": [ { col: value, ... } ]
  }

  ---
  Fabric Notebook — Execution Engine

  notebook/sales_ingest_parameterized.py
  Column 1: Input
  Detail: Parameters injected by Fabric pipeline: input_file, run_id, requested_by. Reads Excel from
    /lakehouse/default/Files/import/{input_file}.
  ────────────────────────────────────────
  Column 1: Process
  Detail: Read Excel → normalize columns → validate required columns → write cleaned data (Parquet + CSV) → compute KPIs

    → compute import profile → write run metadata. All outputs written to both runs/{run_id}/ (immutable) and
    current/ (overwrite).
  ────────────────────────────────────────
  Column 1: Output
  Detail: 5 artifact files per output directory
  Artifact output matrix:

                            runs/{run_id}/    current/
                            ──────────────    ────────
  sales_cleaned.parquet     ✓                 ✓
  sales_cleaned.csv         ✓                 ✓
  kpis.json                 ✓                 ✓
  import_profile.json       ✓                 ✓
  run_metadata.json         ✓ (only)          ✗

  Error escalation strategy within the notebook:
  ┌──────────────────────┬────────────────────────────┐
  │      Operation       │         On failure         │
  ├──────────────────────┼────────────────────────────┤
  │ Read Excel           │ raise RuntimeError (abort) │
  ├──────────────────────┼────────────────────────────┤
  │ Column validation    │ raise RuntimeError (abort) │
  ├──────────────────────┼────────────────────────────┤
  │ Write cleaned data   │ raise RuntimeError (abort) │
  ├──────────────────────┼────────────────────────────┤
  │ Core KPI computation │ raise RuntimeError (abort) │
  ├──────────────────────┼────────────────────────────┤
  │ Optional breakdowns  │ print(Warning) (continue)  │
  ├──────────────────────┼────────────────────────────┤
  │ Write kpis.json      │ raise RuntimeError (abort) │
  ├──────────────────────┼────────────────────────────┤
  │ Import profile       │ print(Warning) (continue)  │
  ├──────────────────────┼────────────────────────────┤
  │ Run metadata         │ print(Warning) (continue)  │
  └──────────────────────┴────────────────────────────┘
  ---
  Page-Level Architecture

  Page 1 — Import (pages/1_Import.py)

  Purpose: File inspection and pipeline trigger.

  End-to-end flow:

  PAGE LOAD
    │
    ├─ locking.is_locked() ──────────────► PostgreSQL: run_lock
    │   └─ If locked: show warning, disable Run button
    │
    ├─ fabric_artifacts.list_import_files() ──► OneLake DFS: Files/import/
    │   └─ Populate selectbox with file names
    │
    └─ Display file metadata (name, size, last_modified)

  USER ACTION: "Inspect File"
    │
    ├─ fabric_artifacts.download_import_file(name) ──► OneLake DFS: Files/import/{name}
    │   └─ Returns raw bytes
    │
    ├─ pd.read_excel() ──► Parse Excel in memory
    │   └─ Normalize column names (strip, lower, underscore)
    │
    └─ kpis.compute_import_profile() ──► Compute schema, validations, sample
        └─ Store in st.session_state["import_profile"]

  USER ACTION: "Run Pipeline"
    │
    ├─ 1. Generate run_id = uuid.uuid4()
    │
    ├─ 2. locking.acquire_lock(run_id, user_name) ──► PostgreSQL: run_lock
    │      └─ UPDATE ... WHERE run_id IS NULL (atomic)
    │      └─ If False: show error, st.stop()
    │
    ├─ 3. fabric_pipelines.trigger_pipeline() ──► Fabric Jobs API
    │      └─ POST /v1/workspaces/{id}/items/{id}/jobs/instances?jobType=Pipeline
    │      └─ Returns location_url from Location header
    │
    ├─ 4. db.insert_pipeline_run() ──► PostgreSQL: pipeline_runs
    │      └─ INSERT with status=SUBMITTED, run_id, location_url, timestamps
    │
    ├─ 5. db.append_event() ──► PostgreSQL: run_events
    │      └─ INSERT STATUS_CHANGE event
    │
    └─ ON FAILURE: locking.release_lock(run_id) ──► PostgreSQL: run_lock

  State transitions initiated: (none) → SUBMITTED

  Lock behavior: Acquired BEFORE trigger. Released in except block if trigger or DB logging fails.

  ---
  Page 2 — Monitor (pages/2_Monitor.py)

  Purpose: Poll Fabric job status, update DB, manage lifecycle transitions.

  End-to-end flow:

  PAGE LOAD
    │
    ├─ locking.is_locked() ──────────────► PostgreSQL: run_lock
    │
    ├─ db.get_active_run() ─────────────► PostgreSQL: pipeline_runs
    │   └─ WHERE status IN (SUBMITTED, QUEUED, RUNNING)
    │   └─ If None: db.list_runs(limit=1) → show most recent run
    │
    └─ Display: status icon, run summary card, timing, IDs

  USER ACTION: "Refresh Status" (or auto-refresh at 30s)
    │
    ├─ 1. fabric_pipelines.poll_job_status(location_url) ──► Fabric Jobs API
    │      └─ GET {location_url}
    │      └─ Returns: { status, raw, http_status }
    │
    ├─ 2. fabric_pipelines.map_fabric_status() ──► Translate Fabric → DB status
    │
    ├─ 3. IF status changed:
    │      │
    │      ├─ db.update_run_status() ──► PostgreSQL: pipeline_runs
    │      │   └─ SET status, started_at/finished_at (conditional), fabric_job_id
    │      │
    │      ├─ db.append_event(STATUS_CHANGE) ──► PostgreSQL: run_events
    │      │
    │      ├─ IF new_status == SUCCEEDED:
    │      │   ├─ fabric_artifacts.get_run_kpis(run_id) ──► OneLake DFS
    │      │   │   └─ Fallback: fabric_artifacts.get_current_kpis()
    │      │   └─ db.update_run_status(kpis=data) ──► PostgreSQL: pipeline_runs.kpis
    │      │       └─ Persists KPI snapshot to JSONB column
    │      │
    │      └─ IF terminal (SUCCEEDED | FAILED):
    │          ├─ locking.release_lock(run_id) ──► PostgreSQL: run_lock
    │          └─ db.append_event(LOG) ──► PostgreSQL: run_events
    │
    └─ 4. Display event timeline from db.get_events(run_id)

  State transitions managed:

  SUBMITTED ──► QUEUED ──► RUNNING ──► SUCCEEDED
                                   └──► FAILED

  KPI persistence (on SUCCEEDED):
  OneLake: runs/{run_id}/kpis.json  ─┐
                                      ├──► db.update_run_status(kpis=...) ──► pipeline_runs.kpis
  OneLake: current/kpis.json ────────┘  (fallback if run-specific artifact unavailable)

  ---
  Page 3 — Results (pages/3_Results.py)

  Purpose: Display latest successful pipeline output.

  End-to-end flow:

  PAGE LOAD
    │
    ├─ CLEANED DATA PREVIEW:
    │   fabric_artifacts.get_current_sample_csv() ──► OneLake DFS
    │     └─ Files/results/current/sales_cleaned.csv
    │     └─ pd.read_csv(nrows=100) → st.dataframe + download button
    │
    └─ KEY PERFORMANCE INDICATORS:
        │
        ├─ PRIMARY: fabric_artifacts.get_current_kpis()
        │   └─ OneLake DFS: Files/results/current/kpis.json
        │
        ├─ FALLBACK: db.get_latest_successful_run()
        │   └─ PostgreSQL: pipeline_runs WHERE status=SUCCEEDED
        │   └─ Read .kpis JSONB column
        │
        └─ RENDER: Core metrics → Funnel → Breakdowns → Time series chart

  Data source priority: OneLake current/ → PostgreSQL JSONB column

  All KPI rendering is type-guarded: isinstance(funnel, dict), isinstance(rev_channel, dict), isinstance(ts, list) —
  prevents crashes on malformed data.

  ---
  Page 4 — Archive (pages/4_Archive.py)

  Purpose: Browse historical runs, compare KPIs, restore past results.

  End-to-end flow:

  PAGE LOAD
    │
    ├─ db.list_runs(limit=100) ──► PostgreSQL: pipeline_runs
    │   └─ Display summary table (Run ID, Status, Triggered By, Input File, Created, Finished)
    │
    ├─ SELECT A RUN:
    │   └─ Display run details card
    │
    ├─ KPIs FOR SELECTED RUN:
    │   ├─ PRIMARY: selected_run.get("kpis") → pipeline_runs.kpis JSONB
    │   ├─ FALLBACK: fabric_artifacts.get_run_kpis(run_id)
    │   │   └─ OneLake DFS: Files/results/runs/{run_id}/kpis.json
    │   └─ RENDER: Core metrics → Funnel → Revenue breakdowns (expandable)
    │
    ├─ EVENT TIMELINE:
    │   └─ db.get_events(run_id) ──► PostgreSQL: run_events
    │
    ├─ RESTORE ACTION:
    │   └─ (see below)
    │
    └─ RESTORE HISTORY:
        └─ db.list_restores(limit=20) ──► PostgreSQL: run_restores

  KPI data source priority (opposite of Results page): PostgreSQL JSONB → OneLake runs/{run_id}/

  Restore flow:

  USER ACTION: "Restore run {id} to current"
    │
    ├─ Guard: selected_run.status must be SUCCEEDED
    ├─ Guard: user_name must be non-empty
    │
    ├─ 1. Generate restore_run_id = uuid.uuid4()
    │
    ├─ 2. fabric_pipelines.trigger_pipeline() ──► Fabric Jobs API
    │      └─ input_file = "__restore__{source_run_id}" (convention signal)
    │      └─ run_id = restore_run_id
    │
    ├─ 3. db.insert_restore() ──► PostgreSQL: run_restores
    │      └─ INSERT (restored_by, source_run_id, target_run_id)
    │      └─ Returns restore_id (UUID)
    │
    ├─ 4. db.append_event() ──► PostgreSQL: run_events
    │      └─ LOG event on source_run_id
    │
    └─ ON FAILURE:
        ├─ db.insert_restore() ──► still log the intent
        └─ db.append_event(WARNING) ──► record failure reason

  ---
  Complete Run Lifecycle — State Transition Map

                      ┌──────────────────────────────────────────────────────────┐
                      │                    IMPORT PAGE                           │
                      │                                                          │
                      │  acquire_lock() ──► trigger_pipeline() ──► insert_run()  │
                      │                     POST to Fabric         status=       │
                      │                     Returns location_url   SUBMITTED     │
                      └──────────────────────────┬───────────────────────────────┘
                                                 │
                      ┌──────────────────────────▼───────────────────────────────┐
                      │                    MONITOR PAGE                          │
                      │                                                          │
                      │  poll_job_status(location_url)                           │
                      │    │                                                     │
                      │    ├─ NotStarted  → update_run_status(QUEUED)            │
                      │    ├─ InProgress  → update_run_status(RUNNING)           │
                      │    │                + set started_at                     │
                      │    ├─ Completed   → update_run_status(SUCCEEDED)         │
                      │    │                + set finished_at                    │
                      │    │                + fetch & persist KPIs to DB         │
                      │    │                + release_lock()                     │
                      │    └─ Failed      → update_run_status(FAILED)            │
                      │                     + set finished_at                    │
                      │                     + release_lock()                     │
                      └──────────────────────────┬───────────────────────────────┘
                                                 │
                ┌────────────────────────────────┴────────────────────────────┐
                │                                                             │
     ┌──────────▼──────────┐                                    ┌─────────────▼──────────┐
     │    RESULTS PAGE     │                                    │     ARCHIVE PAGE       │
     │                     │                                    │                        │
     │  OneLake current/   │                                    │  DB: pipeline_runs     │
     │  ├─ kpis.json       │                                    │  ├─ .kpis (JSONB)      │
     │  ├─ sales_cleaned   │                                    │  ├─ .status             │
     │  └─ (DB fallback)   │                                    │  ├─ run_events          │
     │                     │                                    │  ├─ run_restores        │
     └─────────────────────┘                                    │  └─ OneLake fallback    │
                                                                │     runs/{id}/kpis.json │
                                                                └────────────────────────┘

  ---
  Data Store Contract Summary

  PostgreSQL — Write Operations by Page
  ┌─────────┬───────────────┬─────────────────────────────────────┬────────────────────────────────────────────┐
  │  Page   │     Table     │              Operation              │                    When                    │
  ├─────────┼───────────────┼─────────────────────────────────────┼────────────────────────────────────────────┤
  │ Import  │ run_lock      │ UPDATE (acquire)                    │ Run Pipeline clicked                       │
  ├─────────┼───────────────┼─────────────────────────────────────┼────────────────────────────────────────────┤
  │ Import  │ pipeline_runs │ INSERT                              │ After trigger succeeds                     │
  ├─────────┼───────────────┼─────────────────────────────────────┼────────────────────────────────────────────┤
  │ Import  │ run_events    │ INSERT                              │ After trigger succeeds                     │
  ├─────────┼───────────────┼─────────────────────────────────────┼────────────────────────────────────────────┤
  │ Monitor │ pipeline_runs │ UPDATE (status, timestamps, job_id) │ Each poll with status change               │
  ├─────────┼───────────────┼─────────────────────────────────────┼────────────────────────────────────────────┤
  │ Monitor │ pipeline_runs │ UPDATE (kpis JSONB)                 │ On SUCCEEDED (fetched from OneLake)        │
  ├─────────┼───────────────┼─────────────────────────────────────┼────────────────────────────────────────────┤
  │ Monitor │ run_lock      │ UPDATE (release)                    │ On SUCCEEDED or FAILED                     │
  ├─────────┼───────────────┼─────────────────────────────────────┼────────────────────────────────────────────┤
  │ Monitor │ run_events    │ INSERT                              │ Each status change + lock release + errors │
  ├─────────┼───────────────┼─────────────────────────────────────┼────────────────────────────────────────────┤
  │ Archive │ run_restores  │ INSERT                              │ Restore action                             │
  ├─────────┼───────────────┼─────────────────────────────────────┼────────────────────────────────────────────┤
  │ Archive │ run_events    │ INSERT                              │ Restore action                             │
  └─────────┴───────────────┴─────────────────────────────────────┴────────────────────────────────────────────┘
  OneLake — Read Operations by Page
  ┌─────────┬─────────────────────────────────────────┬──────────────────────────┐
  │  Page   │                  Path                   │         Purpose          │
  ├─────────┼─────────────────────────────────────────┼──────────────────────────┤
  │ Import  │ Files/import/ (listing)                 │ File discovery           │
  ├─────────┼─────────────────────────────────────────┼──────────────────────────┤
  │ Import  │ Files/import/{name} (download)          │ File inspection          │
  ├─────────┼─────────────────────────────────────────┼──────────────────────────┤
  │ Results │ Files/results/current/sales_cleaned.csv │ Data preview             │
  ├─────────┼─────────────────────────────────────────┼──────────────────────────┤
  │ Results │ Files/results/current/kpis.json         │ KPI display              │
  ├─────────┼─────────────────────────────────────────┼──────────────────────────┤
  │ Monitor │ Files/results/runs/{run_id}/kpis.json   │ KPI persistence to DB    │
  ├─────────┼─────────────────────────────────────────┼──────────────────────────┤
  │ Monitor │ Files/results/current/kpis.json         │ KPI persistence fallback │
  ├─────────┼─────────────────────────────────────────┼──────────────────────────┤
  │ Archive │ Files/results/runs/{run_id}/kpis.json   │ Per-run KPI fallback     │
  └─────────┴─────────────────────────────────────────┴──────────────────────────┘
  OneLake — Write Operations (Notebook Only)
  ┌───────────────────────────────────────────────────┬────────────┬──────────────────┐
  │                       Path                        │ Written by │    Immutable     │
  ├───────────────────────────────────────────────────┼────────────┼──────────────────┤
  │ Files/results/runs/{run_id}/sales_cleaned.parquet │ Notebook   │ Yes              │
  ├───────────────────────────────────────────────────┼────────────┼──────────────────┤
  │ Files/results/runs/{run_id}/sales_cleaned.csv     │ Notebook   │ Yes              │
  ├───────────────────────────────────────────────────┼────────────┼──────────────────┤
  │ Files/results/runs/{run_id}/kpis.json             │ Notebook   │ Yes              │
  ├───────────────────────────────────────────────────┼────────────┼──────────────────┤
  │ Files/results/runs/{run_id}/import_profile.json   │ Notebook   │ Yes              │
  ├───────────────────────────────────────────────────┼────────────┼──────────────────┤
  │ Files/results/runs/{run_id}/run_metadata.json     │ Notebook   │ Yes              │
  ├───────────────────────────────────────────────────┼────────────┼──────────────────┤
  │ Files/results/current/sales_cleaned.parquet       │ Notebook   │ No (overwritten) │
  ├───────────────────────────────────────────────────┼────────────┼──────────────────┤
  │ Files/results/current/sales_cleaned.csv           │ Notebook   │ No (overwritten) │
  ├───────────────────────────────────────────────────┼────────────┼──────────────────┤
  │ Files/results/current/kpis.json                   │ Notebook   │ No (overwritten) │
  ├───────────────────────────────────────────────────┼────────────┼──────────────────┤
  │ Files/results/current/import_profile.json         │ Notebook   │ No (overwritten) │
  └───────────────────────────────────────────────────┴────────────┴──────────────────┘
  ---
  Cross-System Dependency Chain — Full Trigger-to-Archive Path

  Step  Actor          System         Operation                     Produces
  ────  ─────          ──────         ─────────                     ────────
   1    Import page    Entra ID       Client-credentials OAuth      Access token (Fabric scope)
   2    Import page    PostgreSQL     acquire_lock()                Lock row updated
   3    Import page    Fabric API     POST jobs/instances           HTTP 202 + Location header
   4    Import page    PostgreSQL     insert_pipeline_run()         pipeline_runs row (SUBMITTED)
   5    Import page    PostgreSQL     append_event()                run_events row
   6    Fabric         Fabric         Pipeline schedules notebook   Notebook execution begins
   7    Notebook       OneLake        Read Files/import/{file}      DataFrame in memory
   8    Notebook       OneLake        Write runs/{id}/*.parquet/csv Immutable cleaned data
   9    Notebook       OneLake        Write runs/{id}/kpis.json     Immutable KPI snapshot
  10    Notebook       OneLake        Write current/*               Overwritten latest artifacts
  11    Notebook       OneLake        Write runs/{id}/metadata.json Run context record
  12    Monitor page   Fabric API     GET {location_url}            Job status JSON
  13    Monitor page   PostgreSQL     update_run_status(RUNNING)    started_at set
  14    Monitor page   Fabric API     GET {location_url}            Completed status
  15    Monitor page   PostgreSQL     update_run_status(SUCCEEDED)  finished_at set
  16    Monitor page   OneLake        GET runs/{id}/kpis.json       KPI dict
  17    Monitor page   PostgreSQL     update_run_status(kpis=...)   kpis JSONB column populated
  18    Monitor page   PostgreSQL     release_lock()                Lock row cleared
  19    Monitor page   PostgreSQL     append_event(LOG)             Lock release recorded
  20    Results page   OneLake        GET current/kpis.json         KPI display (primary)
  21    Results page   PostgreSQL     get_latest_successful_run()   KPI display (fallback)
  22    Results page   OneLake        GET current/sales_cleaned.csv Data preview table
  23    Archive page   PostgreSQL     list_runs()                   Full run history
  24    Archive page   PostgreSQL     selected_run.kpis             Per-run KPI (primary, from step 17)
  25    Archive page   OneLake        GET runs/{id}/kpis.json       Per-run KPI (fallback, from step 9)
  26    Archive page   PostgreSQL     get_events()                  Event timeline
  27    Archive page   Fabric API     trigger_pipeline(__restore__)  Restore action
  28    Archive page   PostgreSQL     insert_restore()              Audit trail
  29    Archive page   PostgreSQL     list_restores()               Restore history display