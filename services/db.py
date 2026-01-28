"""
PostgreSQL connection pool and query helpers.
All interactions with the metadata database go through this module.
"""

import json
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
import psycopg2.pool

from config import get_pg_config

psycopg2.extras.register_uuid()

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        try:
            cfg = get_pg_config()
            _pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=5,
                host=cfg.host,
                port=cfg.port,
                database=cfg.database,
                user=cfg.user,
                password=cfg.password,
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to create database connection pool: {e}"
            ) from e
    return _pool


@contextmanager
def get_conn():
    """Yield a connection from the pool; returns it on exit."""
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


@contextmanager
def get_cursor(commit: bool = True):
    """Yield a dict cursor; auto-commits unless an error occurs."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield cur
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── pipeline_runs ──────────────────────────────────────────────

def insert_pipeline_run(
    run_id: uuid.UUID,
    triggered_by: str,
    input_file: str,
    workspace_id: str,
    pipeline_item_id: str,
    fabric_job_location_url: str,
    status: str = "SUBMITTED",
    app_version: str | None = None,
) -> None:
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs
                (run_id, created_at, triggered_by, input_file,
                 workspace_id, pipeline_item_id,
                 fabric_job_location_url, status, app_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id, _now(), triggered_by, input_file,
                workspace_id, pipeline_item_id,
                fabric_job_location_url, status, app_version,
            ),
        )


def update_run_status(
    run_id: uuid.UUID,
    status: str,
    error_message: str | None = None,
    fabric_job_id: str | None = None,
    kpis: dict | None = None,
) -> None:
    sets = ["status = %s"]
    vals: list[Any] = [status]

    if status == "RUNNING":
        sets.append("started_at = %s")
        vals.append(_now())
    if status in ("SUCCEEDED", "FAILED"):
        sets.append("finished_at = %s")
        vals.append(_now())
    if error_message is not None:
        sets.append("error_message = %s")
        vals.append(error_message)
    if fabric_job_id is not None:
        sets.append("fabric_job_id = %s")
        vals.append(fabric_job_id)
    if kpis is not None:
        sets.append("kpis = %s")
        try:
            vals.append(json.dumps(kpis, default=str))
        except (TypeError, ValueError) as e:
            vals.append(json.dumps({"serialization_error": str(e)}))

    vals.append(run_id)
    with get_cursor() as cur:
        cur.execute(
            f"UPDATE pipeline_runs SET {', '.join(sets)} WHERE run_id = %s",
            vals,
        )


def get_run(run_id: uuid.UUID) -> dict | None:
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT * FROM pipeline_runs WHERE run_id = %s", (run_id,))
        return cur.fetchone()


def get_latest_successful_run() -> dict | None:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT * FROM pipeline_runs
            WHERE status = 'SUCCEEDED'
            ORDER BY finished_at DESC
            LIMIT 1
            """
        )
        return cur.fetchone()


def get_active_run() -> dict | None:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT * FROM pipeline_runs
            WHERE status IN ('SUBMITTED', 'QUEUED', 'RUNNING')
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        return cur.fetchone()


def list_runs(limit: int = 50) -> list[dict]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT * FROM pipeline_runs ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        return cur.fetchall()


# ── run_events ─────────────────────────────────────────────────

def append_event(
    run_id: uuid.UUID,
    event_type: str,
    message: str,
) -> None:
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO run_events (id, run_id, event_time, event_type, message)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (uuid.uuid4(), run_id, _now(), event_type, message),
        )


def get_events(run_id: uuid.UUID) -> list[dict]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT * FROM run_events
            WHERE run_id = %s
            ORDER BY event_time ASC
            """,
            (run_id,),
        )
        return cur.fetchall()


# ── run_artifacts ──────────────────────────────────────────────

def insert_artifact(
    run_id: uuid.UUID,
    artifact_type: str,
    file_path: str,
    file_size: int | None = None,
) -> None:
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO run_artifacts (id, run_id, artifact_type, file_path, file_size, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (uuid.uuid4(), run_id, artifact_type, file_path, file_size, _now()),
        )


def get_artifacts(run_id: uuid.UUID) -> list[dict]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT * FROM run_artifacts WHERE run_id = %s ORDER BY created_at",
            (run_id,),
        )
        return cur.fetchall()


# ── run_restores ───────────────────────────────────────────────

def insert_restore(
    restored_by: str,
    source_run_id: uuid.UUID,
    target_run_id: uuid.UUID | None = None,
) -> uuid.UUID:
    restore_id = uuid.uuid4()
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO run_restores (id, restored_at, restored_by, source_run_id, target_run_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (restore_id, _now(), restored_by, source_run_id, target_run_id),
        )
    return restore_id


def list_restores(limit: int = 50) -> list[dict]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT * FROM run_restores ORDER BY restored_at DESC LIMIT %s",
            (limit,),
        )
        return cur.fetchall()
