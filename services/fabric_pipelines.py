"""
Fabric pipeline trigger and job polling.
Uses the item-based jobs endpoint per fabric_sanity_test.md.
"""

import logging
import time
import uuid
from typing import Any

import requests

from config import get_fabric_config
from services.fabric_auth import get_auth_headers

logger = logging.getLogger(__name__)

_FABRIC_API = "https://api.fabric.microsoft.com/v1"


def trigger_pipeline(
    input_file: str,
    run_id: uuid.UUID,
    requested_by: str,
) -> dict[str, Any]:
    """
    Trigger a Fabric pipeline run via the Jobs API.

    Returns:
        dict with keys:
            - status_code: HTTP status
            - location_url: Job instance polling URL
            - response_headers: full response headers
    """
    cfg = get_fabric_config()

    url = (
        f"{_FABRIC_API}/workspaces/{cfg.workspace_id}"
        f"/items/{cfg.pipeline_id}/jobs/instances"
        f"?jobType=Pipeline"
    )

    payload = {
        "executionData": {
            "OwnerUserPrincipalName": cfg.owner_upn,
            "OwnerUserObjectId": cfg.owner_object_id,
            "parameters": {
                "input_file": input_file,
                "run_id": str(run_id),
                "requested_by": requested_by,
            },
        }
    }

    headers = get_auth_headers()
    resp = requests.post(url, headers=headers, json=payload, timeout=60)

    if resp.status_code != 202:
        raise RuntimeError(
            f"Pipeline trigger failed ({resp.status_code}): {resp.text[:500]}"
        )

    location_url = resp.headers.get("Location", "")
    logger.info("Pipeline triggered. Location: %s", location_url)

    return {
        "status_code": resp.status_code,
        "location_url": location_url,
        "response_headers": dict(resp.headers),
    }


def poll_job_status(location_url: str) -> dict[str, Any]:
    """
    Poll a single time for the job instance status.

    Returns:
        dict with keys:
            - status: job status string (e.g. "InProgress", "Completed", "Failed")
            - raw: full response body (if JSON available)
            - http_status: HTTP status code
    """
    if not location_url:
        raise ValueError("No location URL provided for polling.")

    headers = get_auth_headers()
    resp = requests.get(location_url, headers=headers, timeout=30)

    result: dict[str, Any] = {
        "http_status": resp.status_code,
        "status": "Unknown",
        "raw": {},
    }

    if resp.status_code == 200:
        body = resp.json()
        result["raw"] = body
        result["status"] = body.get("status", "Unknown")
    elif resp.status_code == 202:
        # Still in progress (Fabric returns 202 while running)
        result["status"] = "InProgress"
        try:
            result["raw"] = resp.json()
            result["status"] = result["raw"].get("status", "InProgress")
        except Exception:
            pass
    else:
        result["status"] = "PollError"
        result["error"] = resp.text[:500]

    return result


def map_fabric_status(fabric_status: str) -> str:
    """Map Fabric job status strings to our pipeline_runs.status values."""
    mapping = {
        "NotStarted": "QUEUED",
        "InProgress": "RUNNING",
        "Completed": "SUCCEEDED",
        "Failed": "FAILED",
        "Cancelled": "FAILED",
        "Deduped": "FAILED",
    }
    return mapping.get(fabric_status, "RUNNING")


def poll_until_done(
    location_url: str,
    interval_seconds: int = 15,
    max_polls: int = 120,
) -> dict[str, Any]:
    """
    Poll until the job reaches a terminal state.
    Intended for background/async usage, not the Streamlit UI polling loop.
    """
    for _ in range(max_polls):
        result = poll_job_status(location_url)
        status = result.get("status", "")
        if status in ("Completed", "Failed", "Cancelled", "Deduped"):
            return result
        time.sleep(interval_seconds)
    return {"status": "Timeout", "raw": {}}
