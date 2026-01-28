"""
Transaction-safe run locking via the run_lock table.
Ensures only one pipeline run can be active at a time.
"""

import logging
import uuid
from datetime import datetime, timezone

import psycopg2.extras

from services.db import _get_pool

psycopg2.extras.register_uuid()

logger = logging.getLogger(__name__)


def is_locked() -> tuple[bool, dict | None]:
    """
    Check whether a run lock is currently held.
    Returns (is_locked, lock_row_dict_or_None).
    """
    pool = _get_pool()
    conn = pool.getconn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM run_lock WHERE lock_key = 'ACTIVE_PIPELINE_RUN'"
        )
        row = cur.fetchone()
        cur.close()
        if row and row.get("run_id") is not None:
            return True, dict(row)
        return False, None
    except Exception as e:
        logger.error("Failed to check lock status: %s", e)
        raise
    finally:
        pool.putconn(conn)


def acquire_lock(run_id: uuid.UUID, locked_by: str) -> bool:
    """
    Atomically acquire the run lock.
    Returns True if lock was acquired, False if already held.
    Uses a transactional UPDATE ... WHERE run_id IS NULL pattern.
    """
    pool = _get_pool()
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE run_lock
            SET run_id = %s,
                locked_at = %s,
                locked_by = %s
            WHERE lock_key = 'ACTIVE_PIPELINE_RUN'
              AND run_id IS NULL
            """,
            (run_id, datetime.now(timezone.utc), locked_by),
        )
        acquired = cur.rowcount == 1
        conn.commit()
        cur.close()
        return acquired
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def release_lock(run_id: uuid.UUID) -> bool:
    """
    Release the lock for a specific run_id.
    Returns True if the lock was released, False otherwise.
    """
    pool = _get_pool()
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE run_lock
            SET run_id = NULL,
                locked_at = NULL,
                locked_by = NULL
            WHERE lock_key = 'ACTIVE_PIPELINE_RUN'
              AND run_id = %s
            """,
            (run_id,),
        )
        released = cur.rowcount == 1
        conn.commit()
        cur.close()
        return released
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def force_release_lock() -> bool:
    """
    Unconditionally clear the lock. Use for admin recovery only.
    """
    pool = _get_pool()
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE run_lock
            SET run_id = NULL,
                locked_at = NULL,
                locked_by = NULL
            WHERE lock_key = 'ACTIVE_PIPELINE_RUN'
            """
        )
        conn.commit()
        cur.close()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)
