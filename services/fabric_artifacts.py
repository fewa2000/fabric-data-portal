"""
Artifact reader for Fabric OneLake files.
Fetches small JSON/CSV artifacts produced by the notebook.

Designed with a pluggable interface so we can later swap to direct
OneLake filesystem browsing or Azure SDK access.
"""

import json
import logging
from typing import Any

import requests

from config import get_fabric_config
from services.fabric_auth import get_storage_headers

logger = logging.getLogger(__name__)

_ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"


def _build_onelake_url(file_path: str) -> str:
    """
    Build a OneLake DFS URL for a lakehouse file.
    file_path should be relative, e.g. 'Files/results/current/kpis.json'
    """
    cfg = get_fabric_config()
    # OneLake URL format:
    # https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}/{path}
    return f"{_ONELAKE_DFS}/{cfg.workspace_id}/{cfg.lakehouse_id}/{file_path}"


def read_json_artifact(file_path: str) -> dict[str, Any] | None:
    """
    Read a JSON artifact from OneLake.
    Returns parsed dict or None if not found.
    """
    url = _build_onelake_url(file_path)
    headers = get_storage_headers()
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 404:
            logger.info("Artifact not found: %s", file_path)
            return None
        else:
            logger.warning(
                "Failed to read artifact %s: %s %s",
                file_path, resp.status_code, resp.text[:200],
            )
            return None
    except Exception as e:
        logger.error("Error reading artifact %s: %s", file_path, e)
        return None


def read_csv_artifact(file_path: str) -> str | None:
    """
    Read a CSV artifact from OneLake as raw text.
    Returns CSV text or None if not found.
    """
    url = _build_onelake_url(file_path)
    headers = get_storage_headers()
    # Remove Content-Type for GET
    headers.pop("Content-Type", None)  # storage headers don't include it, but safe to call
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.text
        elif resp.status_code == 404:
            logger.info("CSV artifact not found: %s", file_path)
            return None
        else:
            logger.warning(
                "Failed to read CSV %s: %s %s",
                file_path, resp.status_code, resp.text[:200],
            )
            return None
    except Exception as e:
        logger.error("Error reading CSV %s: %s", file_path, e)
        return None


def list_import_files() -> list[dict[str, Any]]:
    """
    List files in Files/import/ directory.
    Uses the OneLake DFS API to list filesystem contents.
    Returns a list of file info dicts.
    """
    cfg = get_fabric_config()
    url = (
        f"{_ONELAKE_DFS}/{cfg.workspace_id}/{cfg.lakehouse_id}"
        f"/Files/import?resource=filesystem&recursive=false"
    )
    headers = get_storage_headers()
    headers.pop("Content-Type", None)  # storage headers don't include it, but safe to call
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            body = resp.json()
            paths = body.get("paths", [])
            files = []
            for p in paths:
                if not p.get("isDirectory", False):
                    files.append({
                        "name": p.get("name", "").split("/")[-1],
                        "full_path": p.get("name", ""),
                        "size": int(p.get("contentLength", 0)),
                        "last_modified": p.get("lastModified", ""),
                    })
            return files
        else:
            logger.warning(
                "Failed to list import files: %s %s",
                resp.status_code, resp.text[:300],
            )
            return []
    except Exception as e:
        logger.error("Error listing import files: %s", e)
        return []


def download_import_file(file_name: str) -> bytes | None:
    """
    Download a raw import file from OneLake (Files/import/{file_name}).
    Returns file bytes or None if not found / on error.
    """
    url = _build_onelake_url(f"Files/import/{file_name}")
    headers = get_storage_headers()
    try:
        resp = requests.get(url, headers=headers, timeout=60)
        if resp.status_code == 200:
            return resp.content
        elif resp.status_code == 404:
            logger.info("Import file not found: %s", file_name)
            return None
        else:
            logger.warning(
                "Failed to download import file %s: %s %s",
                file_name, resp.status_code, resp.text[:200],
            )
            return None
    except Exception as e:
        logger.error("Error downloading import file %s: %s", file_name, e)
        return None


def get_current_kpis() -> dict[str, Any] | None:
    """Read KPIs from the current results folder."""
    return read_json_artifact("Files/results/current/kpis.json")


def get_current_import_profile() -> dict[str, Any] | None:
    """Read import profile from the current results folder."""
    return read_json_artifact("Files/results/current/import_profile.json")


def get_run_kpis(run_id: str) -> dict[str, Any] | None:
    """Read KPIs for a specific run."""
    return read_json_artifact(f"Files/results/runs/{run_id}/kpis.json")


def get_run_import_profile(run_id: str) -> dict[str, Any] | None:
    """Read import profile for a specific run."""
    return read_json_artifact(f"Files/results/runs/{run_id}/import_profile.json")


def get_run_metadata(run_id: str) -> dict[str, Any] | None:
    """Read run metadata for a specific run."""
    return read_json_artifact(f"Files/results/runs/{run_id}/run_metadata.json")


def get_current_sample_csv() -> str | None:
    """Read sample CSV from current results."""
    return read_csv_artifact("Files/results/current/sales_cleaned.csv")
