You are **Claude Code**, acting as a **Senior Data Engineer and Software Architect**.  
Your task is to design and implement a **production-grade Streamlit Data Portal** that integrates with:

- **Microsoft Fabric** (OneLake + Lakehouse + Notebook + Data Pipeline via REST API)
- A **PostgreSQL metadata database** (run logging, run status, KPI snapshots, run artifacts, audit events, and run locking)

This is not a demo. It must be **multi-user safe**, **traceable**, and **operationally reliable**.

---

## 0) Inputs available in the workspace (must read & use)

You are provided these local workspace files (for context and dev verification):

1) `fabric_sanity_test.md`  
   - Contains working OAuth client-credentials token acquisition  
   - Lists workspace items  
   - Successfully triggers a Fabric Pipeline run via the correct endpoint  
   - Use this script as the reference implementation for Fabric auth + pipeline trigger patterns

2) `sales_orders_fact.xlsx`  
   - Example import file representing the dataset stored in Fabric under:
     `Files/import/sales_orders_fact.xlsx`

3) `data_explained.txt`  
   - Explains the business data and contains funnel validation values that can inform KPI design:
     - Total visitors: 1,428,571
     - Converting visitors: 49,080
     - Orders: 50,000
     - Actual conversion: 3.44%

4) A Fabric_Notebook_Screenshot.png (code will be provided in workspace notes or a file) that currently:
   - Reads Excel from `/lakehouse/default/Files/import/sales_orders_fact.xlsx`
   - Normalizes column names
   - Writes output to `/lakehouse/default/Files/results` as CSV + Parquet

5) `fabric_sales_ingest_notebook.md`  
   - This file represents the **currently working Fabric notebook logic** and must be treated as the **authoritative reference** for:
   - how data is read from OneLake (`/lakehouse/default/Files/import/...`)
   - how column normalization is performed
   - how cleaned data is written back to OneLake
   - expected input/output paths and file formats

You must implement the portal to work with Fabric paths and APIs in production.
Local files are only for dev/test sanity checks.

---

## 1) Target Fabric Lakehouse layout and contracts

Current Lakehouse structure:

```

Files/
├── import/
│    └── sales_orders_fact.xlsx
├── results/

```

### Required results artifact store behavior

After runs, results must be structured like this:

```

Files/
├── import/
├── results/
│    ├── current/
│    │    ├── sales_cleaned.parquet
│    │    ├── sales_cleaned.csv
│    │    ├── kpis.json
│    │    └── import_profile.json
│    ├── runs/
│    │    ├── <run_id>/
│    │    │    ├── sales_cleaned.parquet
│    │    │    ├── sales_cleaned.csv
│    │    │    ├── kpis.json
│    │    │    ├── import_profile.json
│    │    │    └── run_metadata.json

```

Rules:
- `runs/<run_id>/` is immutable per run.
- `current/` is updated only on **successful** runs.
- Each run produces small helper/preview artifacts:
  - `import_profile.json` (schema, row count, date range, last modified, validation summary, sample rows)
  - `kpis.json` (KPI snapshot)

---

## 2) Parameterization requirements (must implement)

The portal must support selecting an import file and running a pipeline with parameters.

### Parameters
- `input_file`: e.g. `sales_orders_fact.xlsx` (selected in UI from Files/import)
- `run_id`: unique execution identifier (timestamp or UUID)
- `requested_by`: user identity string shown in UI and stored in DB

### Notebook contract
Update the notebook (or create a new version) so it:
- reads: `/lakehouse/default/Files/import/{input_file}`
- writes: `/lakehouse/default/Files/results/runs/{run_id}/...`
- on success, also updates `/lakehouse/default/Files/results/current/...`
- writes `import_profile.json`, `kpis.json`, `run_metadata.json`

---

## 3) Streamlit app requirements (pages + UX)

Create a Streamlit application with 4 pages:

### Page 1 — Import
Purpose: inspect before running.
Must show:
- list of available files in `Files/import/`
- selected file metadata: name, size, last modified, row count
- schema preview (columns + inferred dtypes)
- date range (min/max of order_date if present)
- validation results summary
- sample preview (first N rows; use `import_profile.json` sample rows)

Action:
- **Run Pipeline** button
- button disabled if another run is active (multi-user safe locking)

### Page 2 — Monitor
Purpose: monitor pipeline execution and prevent overlap.
Must show:
- current run status: queued/running/succeeded/failed
- polling UI (refresh button + auto-refresh optional)
- Fabric job instance info (Location URL)
- steps/progress (best effort via job instance payload)
- run event timeline from Postgres (`run_events`)
- log links: show notebook output link if discoverable; otherwise show run metadata and Fabric job location

### Page 3 — Results
Purpose: show latest outputs.
Must show:
- KPIs from `Files/results/current/kpis.json`
- preview cleaned data (either from `current/` sample file or provide a small `sample.csv`)
- links to download/view artifacts (CSV/Parquet) if feasible
- show where data comes from: display “Source: Files/results/current/…”

### Page 4 — Archive / History
Purpose: audit & restore.
Must show:
- list of past runs from PostgreSQL (latest first)
- show KPIs per run
- allow selecting a run to view artifacts from `run_artifacts`
- **Restore** action: promote run outputs to `results/current/`
  - implement restore via a Fabric job (preferred), or via a lightweight notebook/pipeline activity
  - write an audit entry to `run_restores`

---

## 4) PostgreSQL metadata database (context only – schema already exists)

The PostgreSQL database and all required tables are **already created and managed externally**.

⚠️ **Important**
- You must **NOT** create tables
- You must **NOT** generate SQL DDL or migration files
- You must **NOT** alter the schema
- You must **assume the schema below exists exactly as described**

Your task is to **connect to the database and interact with the existing tables** using INSERT / UPDATE / SELECT statements only.

---

### Database information

- **Database name:** `fabric_data_portal`
- **Purpose:** Control-plane metadata store for the Streamlit Data Portal
- **Usage:** Run logging, run lifecycle tracking, KPI snapshots, artifact registry, audit trail, and concurrency locking

The database is shared across users and must be treated as the **single source of truth** for application state.

---

### Existing schema (read-only definition for context)

#### A) `pipeline_runs` — central run registry (EXISTS)

One row represents **one Fabric pipeline execution**.

Columns:
- `run_id` (UUID, PK)
- `created_at` (timestamp)
- `started_at` (timestamp, nullable)
- `finished_at` (timestamp, nullable)
- `triggered_by` (text) — user identity from UI
- `input_file` (text)
- `workspace_id` (text)
- `pipeline_item_id` (text)
- `fabric_job_location_url` (text)
- `fabric_job_id` (text, nullable)
- `status` (text, allowed values):
  - `SUBMITTED`
  - `QUEUED`
  - `RUNNING`
  - `SUCCEEDED`
  - `FAILED`
- `status_last_updated_at` (timestamp)
- `error_message` (text, nullable)
- `kpis` (JSONB, nullable) — KPI snapshot produced by the notebook
- `notebook_version` (text, nullable)
- `app_version` (text, nullable)

Usage:
- Insert one row when a run is triggered
- Update status, timestamps, error_message, and kpis during lifecycle
- Read for Monitor, Results, and Archive pages

---

#### B) `run_lock` — global concurrency lock (EXISTS)

Single-row table used to prevent overlapping runs.

Columns:
- `lock_key` (TEXT, PK, fixed value: `ACTIVE_PIPELINE_RUN`)
- `run_id` (UUID, nullable)
- `locked_at` (timestamp, nullable)
- `locked_by` (text, nullable)

Semantics:
- If `run_id IS NOT NULL`, a pipeline run is active
- If `run_id IS NULL`, a new run may be started

Rules for application logic:
- Acquire lock **transactionally** before triggering a pipeline
- Block “Run Pipeline” in UI if lock is held
- Release lock automatically when run finishes (success or failure)

---

#### C) `run_events` — monitoring timeline (EXISTS)

Append-only event log for a run.

Columns:
- `id` (UUID, PK)
- `run_id` (UUID, FK → pipeline_runs)
- `event_time` (timestamp)
- `event_type` (text: `STATUS_CHANGE`, `LOG`, `WARNING`, `ERROR`)
- `message` (text)

Usage:
- Append events during polling and status transitions
- Power the Monitor page timeline
- Never update or delete rows

---

#### D) `run_artifacts` — produced file registry (EXISTS)

Tracks which files were produced by which run.

Columns:
- `id` (UUID, PK)
- `run_id` (UUID, FK → pipeline_runs)
- `artifact_type` (text: `parquet`, `csv`, `json`, `profile`, `metadata`)
- `file_path` (text) — OneLake lakehouse path
- `file_size` (bigint, nullable)
- `created_at` (timestamp)

Usage:
- Insert rows after a successful run
- Used by Archive page and restore logic
- Read-only for UI browsing

---

#### E) `run_restores` — restore audit trail (EXISTS)

Tracks restore actions to `results/current`.

Columns:
- `id` (UUID, PK)
- `restored_at` (timestamp)
- `restored_by` (text)
- `source_run_id` (UUID, FK → pipeline_runs)
- `target_run_id` (UUID, nullable)

Usage:
- Insert one row per restore action
- Used for auditing and traceability
- No updates after insert

---

### Application-level requirements for DB usage

The Streamlit application must implement:

- Connection pooling to PostgreSQL
- Transaction-safe lock acquisition and release (`run_lock`)
- Insert/update operations on `pipeline_runs`
- Append-only inserts into `run_events`
- Artifact registration inserts into `run_artifacts`
- Restore audit inserts into `run_restores`

The database must be treated as **authoritative state**.  
The UI must never rely on in-memory state alone.

---

⚠️ **Do NOT**
- create tables
- modify schema
- generate SQL DDL
- assume schema changes

Only **connect, query, and interact** with the existing schema.

---

## 5) KPI logic requirements

Compute KPIs from cleaned sales data. Minimum KPIs:
- total_revenue = sum(revenue)
- orders = count(order_id)
- aov = total_revenue / orders
- revenue_by_channel
- revenue_by_region
- revenue_by_product_category
- time_series (daily or monthly revenue/orders)

Conversion:
- If `total_visitors` is not present in the file, treat conversion rate as optional.
- If `data_explained.txt` provides funnel values, store them in run_metadata and compute:
  conversion_rate = orders / total_visitors (or converting_visitors / total_visitors depending on definition)
Make the definition explicit in `kpis.json`.

Store KPIs as:
- `kpis.json` in Fabric artifacts
- JSONB `pipeline_runs.kpis` in Postgres

---

## 6) Fabric API integration requirements

### Authentication
- Use client_credentials OAuth with scope:
  `https://api.fabric.microsoft.com/.default`
- Read secrets from Streamlit:
  `.streamlit/secrets.toml`
- Also support environment variables for non-Streamlit execution.

### Pipeline trigger
Must use the item-based jobs endpoint:

`POST /v1/workspaces/{workspaceId}/items/{pipelineItemId}/jobs/instances?jobType=Pipeline`

Body must include Owner fields:
- `OwnerUserPrincipalName`
- `OwnerUserObjectId`

This is required even when using service principal.

Use the proven approach from `fabric_sanity_test.py` as baseline.

### Job polling
- Read `Location` header from pipeline trigger response.
- Poll job instance status using that Location URL.
- Update Postgres status transitions.
- Append entries into `run_events` on each major state transition.

---

## 7) File preview artifacts strategy (confirmed approach)

Do NOT rely on direct OneLake filesystem browsing initially.
Instead:

- Notebook/pipeline writes:
  - `import_profile.json`
  - `kpis.json`
  - optionally `sample.csv` (first 100 rows)
- Streamlit reads only these small artifacts for UI preview.

Implement a service module that can fetch these artifacts from Fabric/OneLake.
Design it with an interface so we can later swap to direct OneLake listing.

---

## 8) Non-functional requirements (production-grade)

- No hardcoded secrets; never commit secrets.
- Robust error handling with user-friendly UI messages.
- Log important events (trigger, poll, status change, failures) to Postgres.
- Idempotency: each run creates a new run_id; never overwrite runs.
- Multi-user safety: DB lock prevents overlapping runs.
- Clean modular code structure.

---

## 9) Deliverables

Create a repository with:

- Streamlit app with pages:
  - Import, Monitor, Results, Archive
- `services/` layer:
  - fabric_auth.py
  - fabric_pipelines.py
  - fabric_artifacts.py (read helper JSONs)
  - db.py (Postgres connection + queries)
  - locking.py (run_lock)
  - kpis.py (KPI compute)
- README with:
  - setup instructions
  - secrets.toml config
  - run workflow explanation
  - troubleshooting guide

---

## 10) Implementation guidance (do this in order)

1) Scaffold Streamlit app + navigation and secrets loading
2) Implement Postgres connection + create tables + run logging
3) Implement run locking
4) Integrate Fabric auth + pipeline trigger (reuse `fabric_sanity_test.py` logic)
5) Implement job polling + status updates + run_events append
6) Update Fabric notebook for parameterization + artifact writing
7) Implement artifact reading (kpis.json/import_profile.json) and show in UI
8) Implement archive browsing + restore action + run_restores audit

---

## 11) Acceptance criteria (must satisfy)

- User can select an import file and view its profile in Import page.
- User can click Run Pipeline; run is logged in Postgres with run_id and job_location.
- A second user cannot start another run while one is active (DB lock).
- Monitor page shows status updates and an event timeline from DB.
- On success, Results page shows KPIs from `current/kpis.json`.
- Archive page lists runs and displays KPI snapshots per run and run artifacts.
- Restore promotes an old run to current and logs the action.

---

### Important
Use the existing `fabric_sanity_test.md` as the canonical reference for Fabric calls and token acquisition patterns.
Use the existing `fabric_sales_ingest_notebook.md` as your reminder of the **currently working Fabric notebook logic**.
Use sql_schema.md as your reminder of the SQL-Schema.
Do not invent undocumented endpoints.
Keep the code production-grade, modular, and readable.
```

---
