# Fabric Data Portal — How It Works

This document explains how the Fabric Data Portal application works, what each screen does, and how data moves through the system from start to finish.

---

## What is this application?

The Fabric Data Portal is a web application that lets you take raw sales data (Excel files), run it through an automated processing pipeline in Microsoft Fabric, and view the resulting business metrics and cleaned data. It provides four screens — Import, Monitor, Results, and Archive — each handling a different stage of the workflow.

The application coordinates three systems:

- **The web interface** (what you see in the browser) — built with Streamlit
- **Microsoft Fabric** — runs the actual data processing in the cloud
- **A PostgreSQL database** — keeps track of every run, its status, and an audit trail

---

## The Four Screens

### Screen 1 — Import

**What it does:** Lets you pick a data file, inspect it before processing, and start a pipeline run.

**What happens when you open the page:**

1. The application checks the database to see if another user is currently running a pipeline. If so, the "Run Pipeline" button is disabled and a warning is shown. This prevents two people from running the pipeline at the same time, which could cause data conflicts.

2. The application connects to OneLake (Fabric's file storage) and lists all files available in the `import` folder. These are the raw Excel files that can be processed.

3. You select a file from the dropdown. The screen shows the file's name, size, and when it was last modified.

**What happens when you click "Inspect File":**

1. The application downloads the selected Excel file from OneLake into memory.
2. It reads the spreadsheet and normalizes the column names (converts them to lowercase, replaces spaces with underscores) — the same transformation the pipeline will perform.
3. It generates a profile of the file:
   - How many rows and columns it contains
   - The data type of each column and how many values are missing
   - The date range covered (earliest and latest order date)
   - Validation checks (e.g., are order IDs unique? Are there negative revenue values?)
   - A preview of the first 10 rows

This lets you verify the data looks correct before committing to a full pipeline run.

**What happens when you click "Run Pipeline":**

1. The application generates a unique Run ID (a random identifier that will track this specific execution).

2. It acquires a lock in the database. This is a safety mechanism — only one pipeline run can be active at a time. The lock records who started it and when. If two people click "Run Pipeline" at the exact same moment, only one will succeed; the other will see an error asking them to wait.

3. It sends a request to Microsoft Fabric's API to start the pipeline. This request includes three parameters: which file to process, the Run ID, and who requested it. Fabric returns a URL that can be used to check the job's progress.

4. It records the new run in the database with a status of "SUBMITTED", along with the Fabric job URL and all identifying information.

5. It logs an event: "Pipeline submitted by [user] for file [filename]".

If anything goes wrong during this process (e.g., Fabric is unreachable), the lock is automatically released so the system doesn't get stuck.

---

### Screen 2 — Monitor

**What it does:** Lets you track a running pipeline and see when it finishes.

**What happens when you open the page:**

1. The application checks the database for any currently active run (one with status SUBMITTED, QUEUED, or RUNNING). If there is no active run, it shows the most recently completed run instead.

2. It displays a summary card with the run's details: which file is being processed, who triggered it, the current status, when it was created/started/finished, and technical identifiers.

**What happens when you click "Refresh Status" (or auto-refresh fires):**

1. The application calls the Fabric API using the job URL saved during the trigger step. Fabric returns the current state of the job.

2. Fabric uses its own status names (e.g., "NotStarted", "InProgress", "Completed"), which the application translates to the portal's terminology:
   - NotStarted becomes **QUEUED** (the job is waiting to execute)
   - InProgress becomes **RUNNING** (the notebook is actively processing data)
   - Completed becomes **SUCCEEDED** (everything finished without errors)
   - Failed/Cancelled becomes **FAILED**

3. If the status has changed since the last check, the application updates the database record and logs a status-change event with a timestamp.

4. When the status reaches **SUCCEEDED**, two important things happen:
   - The application reads the KPI results that the notebook produced (from OneLake) and saves them into the database. This makes the KPIs instantly available on the Archive page without needing to re-fetch them from Fabric's file storage every time.
   - The pipeline lock is released, allowing the next run to proceed.

5. When the status reaches **FAILED**, the lock is also released, but no KPIs are saved.

**The event timeline** at the bottom of the page shows every logged event for this run in chronological order — when it was submitted, when the status changed, when the lock was released. This provides a full audit trail.

---

### Screen 3 — Results

**What it does:** Shows the output from the most recent successful pipeline run.

**What happens when you open the page:**

1. **Cleaned data preview:** The application fetches the cleaned CSV file from OneLake's `results/current/` folder (this folder always contains the output of the latest successful run). It displays the first 100 rows in a table and offers a download button.

2. **Key Performance Indicators:** The application tries two sources for KPI data:
   - First, it reads `kpis.json` from OneLake's `results/current/` folder.
   - If that fails (e.g., the file doesn't exist yet or Fabric storage is temporarily unavailable), it falls back to the database, looking up the most recent successful run's KPI snapshot.

3. The KPIs displayed include:
   - **Total Revenue** — the sum of all revenue in the dataset
   - **Orders** — the number of unique orders
   - **Average Order Value (AOV)** — revenue divided by orders
   - **Conversion Funnel** — how many visitors converted into orders (using reference data: 1,428,571 total visitors)
   - **Revenue by Channel** — breakdown by sales channel (e.g., online, retail)
   - **Revenue by Region** — geographic breakdown
   - **Revenue by Product Category** — what types of products generated the most revenue
   - **Monthly Time Series** — a bar chart showing revenue over time

---

### Screen 4 — Archive

**What it does:** Provides a history of all pipeline runs, lets you compare results across runs, and allows you to restore a past run's output as the current data.

**What happens when you open the page:**

1. The application loads all pipeline runs from the database (up to 100) and displays them in a table showing Run ID, Status, who triggered it, which file was processed, and when it was created and finished.

2. You select a run from a dropdown to see its details.

3. **KPIs for the selected run:** The application first checks the database for saved KPI data (stored there when the Monitor page detected the run succeeded). If not available in the database, it falls back to reading the run-specific `kpis.json` file from OneLake at `results/runs/{run_id}/`. This dual approach means historical KPIs are reliably available even if Fabric storage has intermittent issues.

4. **Event Timeline** (expandable): Shows every event logged for the selected run — submission, status changes, lock release, and any errors.

5. **Restore to Current:** If the selected run succeeded, you can click "Restore" to promote that run's output to `results/current/`. This is useful if a newer run produced incorrect results and you want to revert to an older, known-good dataset. The restore action:
   - Triggers a new Fabric pipeline to copy the selected run's files over the current files
   - Records who performed the restore, when, and which run was the source — creating a permanent audit trail
   - Even if the restore pipeline fails to trigger, the intent is still logged for traceability

6. **Restore History:** Shows a log of all past restore actions.

---

## How the Pipeline Actually Processes Data

When the application triggers a pipeline, Fabric runs a notebook (a script) that performs these steps:

1. **Reads the input file** from `Files/import/` in the Lakehouse (e.g., `sales_orders_fact.xlsx`).

2. **Normalizes column names** — converts headers like "Order ID" to "order_id" for consistency.

3. **Validates the data** — checks that required columns (`revenue`, `order_id`) exist. If they don't, the notebook stops with a clear error.

4. **Writes cleaned data** in two formats:
   - **Parquet** (compact binary format for analytics tools)
   - **CSV** (human-readable format)

   Each is written to two locations:
   - `results/runs/{run_id}/` — an immutable snapshot for this specific run (never overwritten)
   - `results/current/` — the "latest" folder that the Results page reads from (overwritten each time)

5. **Computes KPIs** — calculates total revenue, order counts, average order value, conversion rates, and breakdowns by channel, region, product category, and month.

6. **Writes `kpis.json`** — the computed metrics in a structured format, to both the run folder and current folder.

7. **Writes `import_profile.json`** — schema details, validation results, and sample data from the input file.

8. **Writes `run_metadata.json`** — records the run ID, input file, who requested it, and the timestamp (only in the run folder).

If any critical step fails (reading the file, writing cleaned data, computing core KPIs), the notebook aborts immediately with a descriptive error. Optional steps like revenue breakdowns will log a warning and continue if they fail, so that partial results are still available.

---

## Safety and Reliability Features

### Only one run at a time

The database contains a single-row lock table. Before starting a pipeline, the application atomically claims this lock. "Atomically" means the database guarantees that even if two users click the button in the same millisecond, only one will succeed. The other will see a message asking them to wait.

The lock is released when the pipeline finishes (whether it succeeds or fails). If the application itself crashes, an administrator can manually clear the lock.

### Every action is logged

The database records:
- When each run was created, started, and finished
- Every status change with a timestamp
- Who triggered each run and each restore
- Errors encountered during polling

This creates a complete audit trail that persists independently of Fabric.

### Data is never overwritten destructively

Each run writes to its own folder (`results/runs/{run_id}/`). These folders are never modified after creation. The `results/current/` folder is overwritten only on success, and any past version can be restored from the run-specific archive.

### The database is the source of truth

While Fabric's file storage (OneLake) holds the actual data files, the database is the authoritative record of what happened: which runs occurred, who triggered them, what status they reached, and what KPIs they produced. The application always checks the database first and uses OneLake as a fallback.

### Credentials are never stored in code

All authentication secrets (Fabric API keys, database passwords) are read from a separate secrets file or environment variables. They are never embedded in the source code.

---

## File Storage Layout

The Fabric Lakehouse organizes files like this:

```
Files/
  import/
    sales_orders_fact.xlsx          <-- Raw input files

  results/
    current/                        <-- Latest successful output (overwritten each run)
      sales_cleaned.parquet
      sales_cleaned.csv
      kpis.json
      import_profile.json

    runs/                           <-- Historical snapshots (never modified)
      abc123-def456.../
        sales_cleaned.parquet
        sales_cleaned.csv
        kpis.json
        import_profile.json
        run_metadata.json
      ghi789-jkl012.../
        (same structure)
```

---

## Database Tables

The PostgreSQL database contains five tables:

| Table | Purpose |
|-------|---------|
| **pipeline_runs** | One row per pipeline execution. Stores who triggered it, which file was processed, the status, timestamps, and a snapshot of the computed KPIs. |
| **run_lock** | A single-row table that acts as a traffic light. When a pipeline is running, it holds the run ID. When empty, a new run can start. |
| **run_events** | An append-only log of everything that happens during a run — status changes, lock operations, errors. Used for the event timeline display and auditing. |
| **run_artifacts** | A registry of which files were produced by which run (available for future use). |
| **run_restores** | An audit log of restore actions — who restored which run, and when. |

---

## Complete Workflow — Step by Step

Here is every step from start to finish when a user processes a data file:

| Step | Who | What happens | Where it's recorded |
|------|-----|-------------|-------------------|
| 1 | User | Opens Import page, selects a file | (browser only) |
| 2 | User | Clicks "Inspect File" to preview | (browser session) |
| 3 | User | Clicks "Run Pipeline" | |
| 4 | Application | Claims the pipeline lock | Database: `run_lock` |
| 5 | Application | Sends start request to Fabric | Fabric API returns a tracking URL |
| 6 | Application | Records the new run as SUBMITTED | Database: `pipeline_runs`, `run_events` |
| 7 | User | Navigates to Monitor page | |
| 8 | Fabric | Pipeline starts the notebook | |
| 9 | Notebook | Reads and cleans the Excel file | |
| 10 | Notebook | Writes cleaned Parquet/CSV to run folder and current folder | OneLake storage |
| 11 | Notebook | Computes KPIs and writes kpis.json | OneLake storage |
| 12 | Notebook | Writes import profile and run metadata | OneLake storage |
| 13 | User | Clicks "Refresh Status" on Monitor page | |
| 14 | Application | Polls Fabric for job status | Fabric API |
| 15 | Application | Updates status (e.g., QUEUED, RUNNING) | Database: `pipeline_runs`, `run_events` |
| 16 | User | Clicks "Refresh Status" again | |
| 17 | Application | Detects SUCCEEDED status | |
| 18 | Application | Fetches KPIs from OneLake and saves to database | Database: `pipeline_runs.kpis` |
| 19 | Application | Releases the pipeline lock | Database: `run_lock`, `run_events` |
| 20 | User | Navigates to Results page | |
| 21 | Application | Displays KPIs and cleaned data from current folder | Read from OneLake (database fallback) |
| 22 | User | Navigates to Archive page | |
| 23 | Application | Lists all historical runs with their KPIs | Read from database (OneLake fallback) |
| 24 | User | (Optional) Clicks "Restore" on an older run | |
| 25 | Application | Triggers a restore pipeline and logs the action | Fabric API + Database: `run_restores`, `run_events` |
