"""Demo upstream with configurable response delay and an idempotent cache-clear
ledger.

The cache-clear endpoint is the interesting part: clearing a cache group is
keyed by an operator-supplied operation id. The ledger is written to SQLite
*before* any response delay, so a caller that loses the response (or a gateway
that is killed while waiting) can retry the same operation id and get the first
result back without clearing the group a second time.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import threading
import time
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

NAME = os.environ.get("UPSTREAM_NAME", "upstream")
DEFAULT_DELAY_MS = int(os.environ.get("DEFAULT_DELAY_MS", "0"))
MAX_DELAY_MS = 60_000
DB_PATH = Path(os.environ.get("UPSTREAM_DB", f"/tmp/upstream-{NAME}.db"))

app = FastAPI(title=f"upstream-{NAME}")
_state = {
    "health_fail": os.environ.get("HEALTH_FAIL", "0") == "1",
    "cache_fail": os.environ.get("CACHE_FAIL", "0") == "1",
}

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS cache_groups (
            grp         TEXT PRIMARY KEY,
            clear_count INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS cache_operations (
            operation_id TEXT PRIMARY KEY,
            grp          TEXT NOT NULL,
            result_json  TEXT NOT NULL,
            cleared_at   REAL NOT NULL
        );
        """
    )
    conn.commit()
    return conn


def _db() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = _connect()
    return _conn


def _first_result(row: sqlite3.Row) -> dict:
    return json.loads(row["result_json"])


def clear_group(operation_id: str, group: str) -> tuple[str, dict]:
    """Atomically allocate the clear for (operation_id, group).

    Returns ("created"|"replayed"|"conflict", result-or-conflict-detail).
    Exactly one call can create the clear for an operation id; every later call
    with the same group receives the stored first result unchanged.
    """
    with _lock:
        conn = _db()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT grp, result_json FROM cache_operations WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
            if row is not None:
                conn.commit()
                if row["grp"] != group:
                    return "conflict", {
                        "error": "operation_group_conflict",
                        "operation_id": operation_id,
                        "bound_group": row["grp"],
                        "requested_group": group,
                    }
                return "replayed", _first_result(row)

            conn.execute(
                "INSERT INTO cache_groups (grp, clear_count) VALUES (?, 1) "
                "ON CONFLICT(grp) DO UPDATE SET clear_count = clear_count + 1",
                (group,),
            )
            count = conn.execute(
                "SELECT clear_count FROM cache_groups WHERE grp = ?", (group,)
            ).fetchone()["clear_count"]
            result = {
                "upstream": NAME,
                "operation_id": operation_id,
                "group": group,
                "cleared": True,
                "clear_count": count,
                "cleared_at": time.time(),
            }
            conn.execute(
                "INSERT INTO cache_operations (operation_id, grp, result_json, cleared_at) "
                "VALUES (?, ?, ?, ?)",
                (operation_id, group, json.dumps(result), result["cleared_at"]),
            )
            conn.commit()
            return "created", result
        except Exception:
            conn.rollback()
            raise


def stored_operation(operation_id: str) -> tuple[str, dict]:
    with _lock:
        row = _db().execute(
            "SELECT grp, result_json FROM cache_operations WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
    if row is None:
        return "unknown_operation", {}
    return "found", _first_result(row)


def group_counts() -> dict[str, int]:
    with _lock:
        rows = _db().execute(
            "SELECT grp, clear_count FROM cache_groups ORDER BY grp"
        ).fetchall()
    return {r["grp"]: r["clear_count"] for r in rows}


@app.get("/health")
def health():
    if _state["health_fail"]:
        return JSONResponse(
            status_code=503, content={"status": "failing", "upstream": NAME}
        )
    return {"status": "ok", "upstream": NAME}


class HealthMode(BaseModel):
    fail: bool


@app.post("/admin/health_mode")
def set_health_mode(mode: HealthMode):
    """Demo hook: make /health fail or recover at runtime."""
    _state["health_fail"] = mode.fail
    return {"upstream": NAME, "health_fail": _state["health_fail"]}


class CacheMode(BaseModel):
    fail: bool


@app.post("/admin/cache_mode")
def set_cache_mode(mode: CacheMode):
    """Demo hook: make the cache-clear endpoints temporarily unavailable.

    Used to exercise the gateway's "result unknown / verify after recovery"
    path without killing the upstream process.
    """
    _state["cache_fail"] = mode.fail
    return {"upstream": NAME, "cache_fail": _state["cache_fail"]}


class CacheClearRequest(BaseModel):
    operation_id: str = Field(min_length=1, max_length=128)
    group: str = Field(min_length=1, max_length=128)
    delay_ms: int | None = Field(default=None, ge=0, le=MAX_DELAY_MS)


@app.post("/cache/clear")
async def cache_clear(req: CacheClearRequest):
    """Idempotent cache-group clear.

    The ledger commit happens before the optional response delay, so a lost
    response still leaves the group cleared exactly once. Retries (including
    concurrent ones) return the stored first result and are marked replayed.
    """
    if _state["cache_fail"]:
        return JSONResponse(
            status_code=503,
            content={"upstream": NAME, "error": "cache_temporarily_unavailable"},
        )
    outcome, result = clear_group(req.operation_id, req.group)
    if outcome == "conflict":
        return JSONResponse(status_code=409, content={"upstream": NAME, **result})
    delay = 0 if req.delay_ms is None else max(0, min(req.delay_ms, MAX_DELAY_MS))
    if outcome == "created" and delay:
        # Deliberately after the durable ledger write: the clear is already
        # effective even if this response is lost because the caller timed out.
        await asyncio.sleep(delay / 1000)
    return {**result, "replayed": outcome == "replayed", "response_delay_ms": delay}


@app.get("/cache/clear/{operation_id}")
def cache_query(operation_id: str):
    if _state["cache_fail"]:
        return JSONResponse(
            status_code=503,
            content={"upstream": NAME, "error": "cache_temporarily_unavailable"},
        )
    outcome, result = stored_operation(operation_id)
    if outcome == "unknown_operation":
        return JSONResponse(
            status_code=404,
            content={"upstream": NAME, "error": "unknown_operation",
                     "operation_id": operation_id},
        )
    return {**result, "replayed": True}


@app.get("/cache/groups")
def cache_groups():
    return {"upstream": NAME, "groups": group_counts()}


@app.api_route("/work", methods=["GET", "POST"])
async def work(
    delay_ms: int | None = Query(default=None),
    group: str | None = Query(default=None),
):
    """Existing forwarded workload.

    When `group` is supplied, the response also reports how many times that
    cache group has been cleared on this upstream. This is the observable used
    by the operations page/tests to prove a clear happened exactly once.
    """
    delay = DEFAULT_DELAY_MS if delay_ms is None else max(0, min(delay_ms, MAX_DELAY_MS))
    await asyncio.sleep(delay / 1000)
    body = {"upstream": NAME, "delay_ms": delay, "ts": time.time()}
    counts = group_counts()
    if group is not None:
        body["group"] = group
        body["clear_count"] = counts.get(group, 0)
    else:
        body["clear_counts"] = counts
    return body


@app.get("/")
def root():
    return {
        "upstream": NAME,
        "default_delay_ms": DEFAULT_DELAY_MS,
        "cache_clear_endpoint": "/cache/clear",
    }
