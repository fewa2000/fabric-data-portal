"""
KPI computation logic.
Computes KPIs from cleaned sales data (pandas DataFrame).
Used by the notebook to produce kpis.json.

All KPIs are derived dynamically from the data.
No hardcoded business constants or benchmarks are used.
"""

from typing import Any

import pandas as pd


def compute_kpis(df: pd.DataFrame) -> dict[str, Any]:
    """
    Compute all required KPIs from a cleaned sales DataFrame.
    All metrics are derived dynamically from the file's contents.

    Minimum required columns: order_id, revenue.
    Optional columns for breakdowns: channel, region, product_category, order_date.
    Optional columns for conversion: visitor_id or total_visitors.
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

    # Conversion funnel - derived from data only
    # Look for visitor-related columns to compute conversion
    funnel = _compute_funnel_from_data(df, order_count)
    if funnel:
        kpis["funnel"] = funnel

    # Dynamic breakdowns - detect available categorical columns
    kpis["breakdowns"] = _compute_dynamic_breakdowns(df)

    # Revenue by channel (kept for backward compatibility)
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


def _compute_funnel_from_data(df: pd.DataFrame, order_count: int) -> dict[str, Any] | None:
    """
    Compute conversion funnel metrics dynamically from the data.
    Returns None if no visitor data is available.

    Looks for columns: visitor_id, total_visitors, converting_visitors, visitors.
    """
    funnel: dict[str, Any] = {"orders": order_count}

    # Try to find visitor count from different possible columns
    total_visitors = None
    converting_visitors = None
    definition_parts = []

    # Check for total_visitors column (explicit count)
    if "total_visitors" in df.columns:
        try:
            # If it's a constant column, take the first value
            unique_vals = df["total_visitors"].dropna().unique()
            if len(unique_vals) == 1:
                total_visitors = int(unique_vals[0])
            else:
                # Sum if multiple values
                total_visitors = int(df["total_visitors"].sum())
            definition_parts.append("total_visitors from column")
        except Exception:
            pass

    # Check for visitor_id column (count unique visitors)
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

    # If no visitor data found, return None
    if total_visitors is None:
        return None

    funnel["total_visitors"] = total_visitors

    if converting_visitors is not None:
        funnel["converting_visitors"] = converting_visitors
        # Compute visitor-to-converting conversion rate
        if total_visitors > 0:
            funnel["visitor_conversion_rate_pct"] = round(
                (converting_visitors / total_visitors) * 100, 2
            )

    # Compute order conversion rate
    if total_visitors > 0:
        funnel["conversion_rate_pct"] = round(
            (order_count / total_visitors) * 100, 2
        )

    funnel["definition"] = (
        "conversion_rate = orders / total_visitors; "
        + "; ".join(definition_parts)
    )

    return funnel


def _compute_dynamic_breakdowns(df: pd.DataFrame) -> dict[str, Any]:
    """
    Dynamically detect categorical columns and compute revenue breakdowns.
    Returns a dict with breakdown name -> {category: revenue} mappings.
    """
    breakdowns: dict[str, Any] = {}

    if "revenue" not in df.columns:
        return breakdowns

    # Identify potential categorical columns for breakdown
    # Exclude known non-categorical columns
    exclude_cols = {
        "revenue", "order_id", "visitor_id", "order_date", "date",
        "total_visitors", "converting_visitors", "visitors",
        "quantity", "price", "amount", "total", "subtotal",
    }

    for col in df.columns:
        col_lower = col.lower()
        if col_lower in exclude_cols:
            continue

        # Check if column is suitable for breakdown (categorical-like)
        try:
            unique_count = df[col].nunique()
            row_count = len(df)

            # Skip if too many unique values (likely not categorical)
            # or if it's all unique (likely an ID column)
            if unique_count > 50 or unique_count == row_count:
                continue

            # Skip if too few categories
            if unique_count < 2:
                continue

            # Skip numeric columns that look like IDs or continuous values
            if df[col].dtype in ("int64", "float64"):
                # Check if values look like IDs (sequential or random large numbers)
                if df[col].min() > 1000 or unique_count > 20:
                    continue

            # Compute revenue breakdown for this column
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
