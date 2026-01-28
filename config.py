"""
Centralized configuration loader.
Reads from Streamlit secrets.toml first, falls back to environment variables.
"""

import os
from dataclasses import dataclass

_USE_STREAMLIT = True
try:
    import streamlit as st
except ImportError:
    _USE_STREAMLIT = False


def _get(key: str, default: str = "") -> str:
    """Read a config value from Streamlit secrets or env vars."""
    if _USE_STREAMLIT:
        try:
            val = st.secrets.get(key, "")
            if val:
                return str(val)
        except Exception:
            pass
    return os.getenv(key, default)


@dataclass(frozen=True)
class FabricConfig:
    tenant_id: str
    client_id: str
    client_secret: str
    workspace_id: str
    pipeline_id: str
    lakehouse_id: str
    owner_upn: str
    owner_object_id: str


@dataclass(frozen=True)
class PgConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


def get_fabric_config() -> FabricConfig:
    return FabricConfig(
        tenant_id=_get("FABRIC_TENANT_ID"),
        client_id=_get("FABRIC_CLIENT_ID"),
        client_secret=_get("FABRIC_CLIENT_SECRET"),
        workspace_id=_get("FABRIC_WORKSPACE_ID"),
        pipeline_id=_get("FABRIC_PIPELINE_ID"),
        lakehouse_id=_get("FABRIC_LAKEHOUSE_ID"),
        owner_upn=_get("OWNER_UPN"),
        owner_object_id=_get("OWNER_OBJECT_ID"),
    )


def get_pg_config() -> PgConfig:
    try:
        port = int(_get("PG_PORT", "5432"))
    except (ValueError, TypeError):
        port = 5432
    return PgConfig(
        host=_get("PG_HOST", "localhost"),
        port=port,
        database=_get("PG_DATABASE", "fabric_data_portal"),
        user=_get("PG_USER"),
        password=_get("PG_PASSWORD"),
    )


def get_app_version() -> str:
    return _get("APP_VERSION", "1.0.0")
