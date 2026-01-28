# Fabric Data Portal

Production-grade Streamlit application for managing sales data pipelines through Microsoft Fabric, with PostgreSQL-backed run tracking, concurrency locking, and full audit trails.

## Overview

```
Streamlit UI  ──►  Fabric REST API  ──►  Pipeline  ──►  Notebook
     │                                                      │
     ▼                                                      ▼
 PostgreSQL                                             OneLake
 (run metadata,                                        (data files,
  locking, audit)                                       KPIs, artifacts)
```

The portal provides a four-page workflow:

| Page | Purpose |
|------|---------|
| **Import** | Inspect files in OneLake, preview schema and validations, trigger pipeline runs |
| **Monitor** | Poll Fabric job status, track state transitions, view event timeline |
| **Results** | View KPIs, cleaned data preview, download artifacts from the latest run |
| **Archive** | Browse run history, compare KPIs across runs, restore past results |

## Prerequisites

- Python 3.11+
- PostgreSQL database with [schema already created](sql_schema.md)
- Microsoft Fabric workspace with a Lakehouse and Data Pipeline
- Entra ID app registration (client credentials grant)

## Setup

**1. Install dependencies**

```bash
pip install -r requirements.txt
```

**2. Configure secrets**

Create `.streamlit/secrets.toml`:

```toml
# Microsoft Fabric / Entra ID
FABRIC_TENANT_ID = "your-tenant-id"
FABRIC_CLIENT_ID = "your-client-id"
FABRIC_CLIENT_SECRET = "your-client-secret"
FABRIC_WORKSPACE_ID = "your-workspace-id"
FABRIC_PIPELINE_ID = "your-pipeline-item-id"
FABRIC_LAKEHOUSE_ID = "your-lakehouse-item-id"
OWNER_UPN = "user@domain.com"
OWNER_OBJECT_ID = "user-object-id"

# PostgreSQL
PG_HOST = "your-pg-host"
PG_PORT = "5432"
PG_DATABASE = "fabric_data_portal"
PG_USER = "your-pg-user"
PG_PASSWORD = "your-pg-password"
```

**3. Run**

```bash
streamlit run Home.py
```

## Project Structure

```
fabric-data-portal/
├── Home.py                             # App entry point
├── config.py                           # Secrets & environment config
├── pages/
│   ├── 1_Import.py
│   ├── 2_Monitor.py
│   ├── 3_Results.py
│   └── 4_Archive.py
├── services/
│   ├── db.py                           # PostgreSQL connection pool & queries
│   ├── locking.py                      # Atomic run lock (single-row table)
│   ├── fabric_auth.py                  # Entra ID OAuth token manager
│   ├── fabric_pipelines.py             # Pipeline trigger & job polling
│   ├── fabric_artifacts.py             # OneLake DFS file reader
│   └── kpis.py                         # KPI computation engine
├── notebook/
│   └── sales_ingest_parameterized.py   # Fabric notebook (runs inside Fabric)
├── requirements.txt
└── sql_schema.md                       # PostgreSQL schema reference
```

## Lakehouse Layout

```
Files/
├── import/
│   └── sales_orders_fact.xlsx        # Raw input
└── results/
    ├── current/                      # Latest successful output (overwritten)
    │   ├── sales_cleaned.parquet
    │   ├── sales_cleaned.csv
    │   ├── kpis.json
    │   └── import_profile.json
    └── runs/<run_id>/                # Immutable per-run snapshots
        ├── sales_cleaned.parquet
        ├── sales_cleaned.csv
        ├── kpis.json
        ├── import_profile.json
        └── run_metadata.json
```

## Key Design Decisions

- **Concurrency safety** — A single-row `run_lock` table with atomic `UPDATE ... WHERE run_id IS NULL` prevents overlapping pipeline runs across multiple users.
- **Dual KPI storage** — KPIs are persisted both as OneLake JSON artifacts and in the PostgreSQL `pipeline_runs.kpis` JSONB column. Each page uses one as primary, the other as fallback.
- **Immutable run snapshots** — Every run writes to `results/runs/<run_id>/` which is never modified. `results/current/` is overwritten only on success.
- **Append-only audit log** — All status changes, errors, lock operations, and restore actions are recorded in `run_events` and `run_restores`.

## Documentation

| Document | Audience | Content |
|----------|----------|---------|
| [docu.md](docu.md) | General | Plain-language walkthrough of every screen and workflow |
| [tech_docu.md](tech_docu.md) | Engineering | Module-level architecture, data flow diagrams, function inventories, state machines |
| [sql_schema.md](sql_schema.md) | Engineering | PostgreSQL table definitions |

## Troubleshooting

| Problem | Check |
|---------|-------|
| Token acquisition fails | Verify `FABRIC_TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`; ensure Entra app has Fabric API permissions |
| Pipeline trigger returns non-202 | Verify `WORKSPACE_ID`, `PIPELINE_ID`, `OWNER_UPN`, `OWNER_OBJECT_ID` |
| Database connection fails | Verify PG_* connection parameters; ensure schema exists per `sql_schema.md` |
| Lock stuck after crash | Run: `UPDATE run_lock SET run_id=NULL, locked_at=NULL, locked_by=NULL WHERE lock_key='ACTIVE_PIPELINE_RUN';` |
| No import files listed | Ensure files exist in `Files/import/` and `FABRIC_LAKEHOUSE_ID` is correct |

## License

Private repository. All rights reserved.
