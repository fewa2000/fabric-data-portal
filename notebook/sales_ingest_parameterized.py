"""
Parameterized Fabric Notebook — Sales Data Ingest & KPI Computation.

This notebook is designed to run inside Microsoft Fabric.
It reads parameters from the pipeline and produces:
  - sales_cleaned.parquet
  - sales_cleaned.csv
  - kpis.json
  - import_profile.json
  - run_metadata.json

Outputs are written to:
  - /lakehouse/default/Files/results/runs/{run_id}/
  - /lakehouse/default/Files/results/current/ (on success)

Parameters (passed from pipeline):
  - input_file: filename in Files/import/ (e.g. sales_orders_fact.xlsx)
  - run_id: unique run identifier
  - requested_by: user who triggered the run
"""

import json
import os
from datetime import datetime, timezone

import pandas as pd


# ══════════════════════════════════════════════════════════════════
# Helper functions for dynamic KPI computation
# All metrics derived from data - no hardcoded constants
# ══════════════════════════════════════════════════════════════════


def _compute_funnel_from_data(df, order_count):
    """
    Compute conversion funnel metrics dynamically from the data.
    Returns None if no visitor data is available.
    """
    funnel = {"orders": order_count}
    total_visitors = None
    converting_visitors = None
    definition_parts = []

    # Check for total_visitors column
    if "total_visitors" in df.columns:
        try:
            unique_vals = df["total_visitors"].dropna().unique()
            if len(unique_vals) == 1:
                total_visitors = int(unique_vals[0])
            else:
                total_visitors = int(df["total_visitors"].sum())
            definition_parts.append("total_visitors from column")
        except Exception:
            pass

    # Check for visitor_id column
    if total_visitors is None and "visitor_id" in df.columns:
        try:
            total_visitors = int(df["visitor_id"].nunique())
            definition_parts.append("total_visitors = unique visitor_id count")
        except Exception:
            pass

    # Check for visitors column
    if total_visitors is None and "visitors" in df.columns:
        try:
            unique_vals = df["visitors"].dropna().unique()
            if len(unique_vals) == 1:
                total_visitors = int(unique_vals[0])
            else:
                total_visitors = int(df["visitors"].sum())
            definition_parts.append("total_visitors from visitors column")
        except Exception:
            pass

    # Check for converting_visitors column
    if "converting_visitors" in df.columns:
        try:
            unique_vals = df["converting_visitors"].dropna().unique()
            if len(unique_vals) == 1:
                converting_visitors = int(unique_vals[0])
            else:
                converting_visitors = int(df["converting_visitors"].sum())
            definition_parts.append("converting_visitors from column")
        except Exception:
            pass

    if total_visitors is None:
        return None

    funnel["total_visitors"] = total_visitors

    if converting_visitors is not None:
        funnel["converting_visitors"] = converting_visitors
        if total_visitors > 0:
            funnel["visitor_conversion_rate_pct"] = round(
                (converting_visitors / total_visitors) * 100, 2
            )

    if total_visitors > 0:
        funnel["conversion_rate_pct"] = round(
            (order_count / total_visitors) * 100, 2
        )

    funnel["definition"] = (
        "conversion_rate = orders / total_visitors; "
        + "; ".join(definition_parts)
    )
    return funnel


def _compute_dynamic_breakdowns(df):
    """
    Dynamically detect categorical columns and compute revenue breakdowns.
    """
    breakdowns = {}
    if "revenue" not in df.columns:
        return breakdowns

    exclude_cols = {
        "revenue", "order_id", "visitor_id", "order_date", "date",
        "total_visitors", "converting_visitors", "visitors",
        "quantity", "price", "amount", "total", "subtotal",
    }

    for col in df.columns:
        col_lower = col.lower()
        if col_lower in exclude_cols:
            continue

        try:
            unique_count = df[col].nunique()
            row_count = len(df)

            if unique_count > 50 or unique_count == row_count:
                continue
            if unique_count < 2:
                continue

            if df[col].dtype in ("int64", "float64"):
                if df[col].min() > 1000 or unique_count > 20:
                    continue

            rev_breakdown = (
                df.groupby(col)["revenue"]
                .sum()
                .round(2)
                .sort_values(ascending=False)
            )
            breakdowns[f"revenue_by_{col}"] = rev_breakdown.to_dict()
        except Exception:
            continue

    return breakdowns


# ══════════════════════════════════════════════════════════════════
# CELL 1 — Parameters cell (mark this cell as "Parameters" in Fabric)
#
# When the pipeline invokes this notebook, Fabric injects a new cell
# AFTER this one that overrides these variables with the actual values.
# These defaults are used only for manual / interactive runs.
# ══════════════════════════════════════════════════════════════════
input_file = "sales_orders_fact.xlsx"
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
requested_by = "notebook-direct"

# ══════════════════════════════════════════════════════════════════
# CELL 2 — Execution starts here
# ══════════════════════════════════════════════════════════════════
print(f"Parameters: input_file={input_file}, run_id={run_id}, requested_by={requested_by}")

# ── Paths ────────────────────────────────────────────────────────
BASE = "/lakehouse/default/Files"
INPUT_PATH = f"{BASE}/import/{input_file}"
RUN_DIR = f"{BASE}/results/runs/{run_id}"
CURRENT_DIR = f"{BASE}/results/current"

# Ensure output directories exist
os.makedirs(RUN_DIR, exist_ok=True)
os.makedirs(CURRENT_DIR, exist_ok=True)

# ── Read input ───────────────────────────────────────────────────
print(f"Reading: {INPUT_PATH}")
try:
    # Support both Excel and CSV files
    if input_file.lower().endswith(".csv"):
        df = pd.read_csv(INPUT_PATH)
    else:
        df = pd.read_excel(INPUT_PATH)
except FileNotFoundError:
    raise RuntimeError(f"Input file not found: {INPUT_PATH}")
except Exception as e:
    raise RuntimeError(f"Failed to read input file {INPUT_PATH}: {e}")
print(f"Rows: {len(df)}")

# ── Normalize columns ───────────────────────────────────────────
df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
print(f"Columns: {list(df.columns)}")

# ── Validate required columns ────────────────────────────────────
required_cols = ["revenue", "order_id"]
missing_cols = [c for c in required_cols if c not in df.columns]
if missing_cols:
    raise RuntimeError(
        f"Missing required columns after normalization: {missing_cols}. "
        f"Available columns: {list(df.columns)}"
    )

# ── Write cleaned data ──────────────────────────────────────────
try:
    # Run-specific (immutable)
    df.to_parquet(f"{RUN_DIR}/sales_cleaned.parquet", index=False)
    df.to_csv(f"{RUN_DIR}/sales_cleaned.csv", index=False)

    # Current (overwritten on success)
    df.to_parquet(f"{CURRENT_DIR}/sales_cleaned.parquet", index=False)
    df.to_csv(f"{CURRENT_DIR}/sales_cleaned.csv", index=False)
except Exception as e:
    raise RuntimeError(f"Failed to write cleaned data files: {e}")

print("Cleaned data written.")

# ── Compute KPIs ────────────────────────────────────────────────
# All KPIs are derived dynamically from the data - no hardcoded constants

try:
    total_revenue = float(df["revenue"].sum())
    order_count = int(df["order_id"].nunique())
except Exception as e:
    raise RuntimeError(f"Failed to compute core KPIs: {e}")

aov = total_revenue / order_count if order_count > 0 else 0.0

kpis = {
    "total_revenue": round(total_revenue, 2),
    "orders": order_count,
    "aov": round(aov, 2),
}

# Conversion funnel - derived from data only (no hardcoded constants)
funnel = _compute_funnel_from_data(df, order_count)
if funnel:
    kpis["funnel"] = funnel

# Dynamic breakdowns for all detected categorical columns
kpis["breakdowns"] = _compute_dynamic_breakdowns(df)

# Revenue by channel
if "channel" in df.columns:
    try:
        kpis["revenue_by_channel"] = (
            df.groupby("channel")["revenue"].sum().round(2)
            .sort_values(ascending=False).to_dict()
        )
    except Exception as e:
        print(f"Warning: Failed to compute revenue by channel: {e}")

# Revenue by region
if "region" in df.columns:
    try:
        kpis["revenue_by_region"] = (
            df.groupby("region")["revenue"].sum().round(2)
            .sort_values(ascending=False).to_dict()
        )
    except Exception as e:
        print(f"Warning: Failed to compute revenue by region: {e}")

# Revenue by product category
if "product_category" in df.columns:
    try:
        kpis["revenue_by_product_category"] = (
            df.groupby("product_category")["revenue"].sum().round(2)
            .sort_values(ascending=False).to_dict()
        )
    except Exception as e:
        print(f"Warning: Failed to compute revenue by product category: {e}")

# Monthly time series
if "order_date" in df.columns:
    try:
        df_ts = df.copy()
        df_ts["order_date"] = pd.to_datetime(df_ts["order_date"], errors="coerce")
        df_ts["month"] = df_ts["order_date"].dt.to_period("M").astype(str)
        monthly = (
            df_ts.groupby("month")
            .agg(revenue=("revenue", "sum"), orders=("order_id", "nunique"))
            .reset_index()
        )
        monthly["revenue"] = monthly["revenue"].round(2)
        kpis["time_series_monthly"] = monthly.to_dict(orient="records")
    except Exception as e:
        print(f"Warning: Failed to compute monthly time series: {e}")

# Write KPIs
try:
    for d in [RUN_DIR, CURRENT_DIR]:
        with open(f"{d}/kpis.json", "w") as f:
            json.dump(kpis, f, indent=2, default=str)
except Exception as e:
    raise RuntimeError(f"Failed to write kpis.json: {e}")

print("KPIs written.")

# ── Compute import profile ──────────────────────────────────────
try:
    profile = {
        "file_name": input_file,
        "file_size": os.path.getsize(INPUT_PATH) if os.path.exists(INPUT_PATH) else None,
        "last_modified": None,
        "row_count": len(df),
        "column_count": len(df.columns),
    }

    # Schema
    schema = []
    for col in df.columns:
        schema.append({
            "column": col,
            "dtype": str(df[col].dtype),
            "null_count": int(df[col].isnull().sum()),
            "null_pct": round(float(df[col].isnull().mean()) * 100, 2),
        })
    profile["schema"] = schema

    # Date range
    if "order_date" in df.columns:
        dates = pd.to_datetime(df["order_date"], errors="coerce")
        profile["date_range"] = {
            "min": str(dates.min()),
            "max": str(dates.max()),
        }

    # Validations
    validations = []
    if "order_id" in df.columns:
        dup_count = int(df["order_id"].duplicated().sum())
        validations.append({
            "check": "order_id_unique",
            "passed": dup_count == 0,
            "detail": f"{dup_count} duplicates found",
        })
    if "revenue" in df.columns:
        neg_count = int((df["revenue"] < 0).sum())
        validations.append({
            "check": "revenue_non_negative",
            "passed": neg_count == 0,
            "detail": f"{neg_count} negative values found",
        })
    null_rows = int(df.isnull().any(axis=1).sum())
    validations.append({
        "check": "no_null_rows",
        "passed": null_rows == 0,
        "detail": f"{null_rows} rows with nulls",
    })
    profile["validations"] = validations

    # Sample rows
    sample = df.head(10).copy()
    for col in sample.columns:
        if sample[col].dtype.name.startswith("datetime"):
            sample[col] = sample[col].astype(str)
    profile["sample_rows"] = sample.to_dict(orient="records")

    for d in [RUN_DIR, CURRENT_DIR]:
        with open(f"{d}/import_profile.json", "w") as f:
            json.dump(profile, f, indent=2, default=str)
except Exception as e:
    print(f"Warning: Failed to write import profile: {e}")

print("Import profile written.")

# ── Write run metadata ──────────────────────────────────────────
try:
    run_metadata = {
        "run_id": run_id,
        "input_file": input_file,
        "requested_by": requested_by,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "row_count": len(df),
        "columns": list(df.columns),
        "kpi_summary": {
            "total_revenue": kpis.get("total_revenue"),
            "orders": kpis.get("orders"),
            "aov": kpis.get("aov"),
            "has_funnel_data": "funnel" in kpis,
        },
    }

    with open(f"{RUN_DIR}/run_metadata.json", "w") as f:
        json.dump(run_metadata, f, indent=2, default=str)
except Exception as e:
    print(f"Warning: Failed to write run metadata: {e}")

print(f"Run metadata written to {RUN_DIR}/run_metadata.json")
print("Notebook complete.")
