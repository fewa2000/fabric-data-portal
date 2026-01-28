"""
KPI computation logic.
Computes KPIs from cleaned sales data (pandas DataFrame).
Used by the notebook to produce kpis.json.
"""

from typing import Any

import pandas as pd

# Funnel constants from data_explained.txt
FUNNEL_TOTAL_VISITORS = 1_428_571
FUNNEL_CONVERTING_VISITORS = 49_080


def compute_kpis(df: pd.DataFrame) -> dict[str, Any]:
    """
    Compute all required KPIs from a cleaned sales DataFrame.
    Expected columns (normalized): order_id, order_date, region,
        product_category, channel, revenue, visitor_id.
    """
    kpis: dict[str, Any] = {}

    # Validate required columns exist
    missing = [c for c in ("revenue", "order_id") if c not in df.columns]
    if missing:
        kpis["error"] = f"Missing required columns: {missing}"
        kpis["total_revenue"] = 0
        kpis["orders"] = 0
        kpis["aov"] = 0
        return kpis

    # Core metrics
    try:
        total_revenue = float(df["revenue"].sum())
        order_count = int(df["order_id"].nunique())
    except Exception as e:
        kpis["error"] = f"Failed to compute core metrics: {e}"
        kpis["total_revenue"] = 0
        kpis["orders"] = 0
        kpis["aov"] = 0
        return kpis

    aov = total_revenue / order_count if order_count > 0 else 0.0

    kpis["total_revenue"] = round(total_revenue, 2)
    kpis["orders"] = order_count
    kpis["aov"] = round(aov, 2)

    # Conversion funnel
    kpis["funnel"] = {
        "total_visitors": FUNNEL_TOTAL_VISITORS,
        "converting_visitors": FUNNEL_CONVERTING_VISITORS,
        "orders": order_count,
        "conversion_rate_pct": round(
            (order_count / FUNNEL_TOTAL_VISITORS) * 100, 2
        )
        if FUNNEL_TOTAL_VISITORS > 0
        else None,
        "definition": "conversion_rate = orders / total_visitors",
    }

    # Revenue by channel
    if "channel" in df.columns:
        try:
            rev_by_channel = (
                df.groupby("channel")["revenue"]
                .sum()
                .round(2)
                .sort_values(ascending=False)
            )
            kpis["revenue_by_channel"] = rev_by_channel.to_dict()
        except Exception:
            kpis["revenue_by_channel"] = {}

    # Revenue by region
    if "region" in df.columns:
        try:
            rev_by_region = (
                df.groupby("region")["revenue"]
                .sum()
                .round(2)
                .sort_values(ascending=False)
            )
            kpis["revenue_by_region"] = rev_by_region.to_dict()
        except Exception:
            kpis["revenue_by_region"] = {}

    # Revenue by product category
    if "product_category" in df.columns:
        try:
            rev_by_cat = (
                df.groupby("product_category")["revenue"]
                .sum()
                .round(2)
                .sort_values(ascending=False)
            )
            kpis["revenue_by_product_category"] = rev_by_cat.to_dict()
        except Exception:
            kpis["revenue_by_product_category"] = {}

    # Time series (monthly)
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
        except Exception:
            kpis["time_series_monthly"] = []

    return kpis


def compute_import_profile(
    df: pd.DataFrame,
    file_name: str,
    file_size: int | None = None,
    last_modified: str | None = None,
) -> dict[str, Any]:
    """
    Compute the import profile for a file.
    Returns schema, row count, date range, validation summary, sample rows.
    """
    profile: dict[str, Any] = {
        "file_name": file_name,
        "file_size": file_size,
        "last_modified": last_modified,
        "row_count": len(df),
        "column_count": len(df.columns),
    }

    # Schema
    try:
        schema = []
        for col in df.columns:
            schema.append({
                "column": col,
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isnull().sum()),
                "null_pct": round(float(df[col].isnull().mean()) * 100, 2),
            })
        profile["schema"] = schema
    except Exception:
        profile["schema"] = []

    # Date range
    if "order_date" in df.columns:
        try:
            dates = pd.to_datetime(df["order_date"], errors="coerce")
            profile["date_range"] = {
                "min": str(dates.min()),
                "max": str(dates.max()),
            }
        except Exception:
            profile["date_range"] = {"min": "N/A", "max": "N/A"}

    # Validation summary
    validations = []
    try:
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
    except Exception:
        validations.append({
            "check": "validation_error",
            "passed": False,
            "detail": "Validation computation failed",
        })
    profile["validations"] = validations

    # Sample rows (first 10)
    try:
        sample = df.head(10).copy()
        for col in sample.columns:
            if sample[col].dtype.name.startswith("datetime"):
                sample[col] = sample[col].astype(str)
        profile["sample_rows"] = sample.to_dict(orient="records")
    except Exception:
        profile["sample_rows"] = []

    return profile
