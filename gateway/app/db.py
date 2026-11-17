"""SQLite persistence for the edge gateway.

Tables:
  upstreams  - registered upstream targets (seeded from env, upserted on boot)
  routes     - single-row table holding the effective route and its version
  inflight   - one row per admitted proxied request; wiped on startup so a
               restarted gateway never reports interrupted requests as in-flight
  probes     - audit log of candidate probes; a commit requires a fresh,
               successful probe recorded against the same route version
  cache_operations
             - idempotent cache-clear operations keyed by operator operation id.
               The row is created (with the *then current* target URL) before
               the upstream call, persists across gateway restarts, and is
               never re-pointed by a later switch. status is one of:
                 pending     - admitted, upstream outcome not recorded yet
                 done        - first result known; retries replay it
                 conflict    - same operation id attempted with another group
                 unknown     - upstream unreachable/lost response: result
                               unverified
                 not_cleared - original upstream confirms it never executed
                               this operation id
               A target with an unresolved (pending/unknown) operation must not
               be reported as safe to take offline.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path

DB_PATH = Path(os.environ.get("GATEWAY_DB", "/data/gateway.db"))
PROBE_TTL_SECONDS = float(os.environ.get("PROBE_TTL_SECONDS", "60"))
# Upper bound on a leased cache-clear attempt. Longer than any configured
# upstream timeout, so it only guards against orphaned in-flight rows.
ATTEMPT_LEASE_SECONDS = float(os.environ.get("CACHE_ATTEMPT_LEASE_SECONDS", "180"))

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(upstreams: dict[str, str], initial_target: str) -> dict:
    """Create tables, upsert the upstream registry, seed the route once,
    and drop in-flight rows left behind by a previous (killed) process.

    Cache operations are deliberately *not* cleared on restart: a retry after
    a gateway restart must still replay the first result, and an unresolved
    operation must keep blocking an offline hint until it is verified.
    """
    global _conn
    if not upstreams:
        raise RuntimeError("no upstreams configured")
    if initial_target not in upstreams:
        raise RuntimeError(f"initial target {initial_target!r} not in upstreams")
    _conn = _connect()
    with _lock, _conn:
        _conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS upstreams (
                name TEXT PRIMARY KEY,
                url  TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS routes (
                id             INTEGER PRIMARY KEY CHECK (id = 1),
                current_target TEXT NOT NULL,
                version        INTEGER NOT NULL,
                updated_at     REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS inflight (
                id         TEXT PRIMARY KEY,
                target     TEXT NOT NULL,
                path       TEXT NOT NULL,
                started_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS probes (
                id           TEXT PRIMARY KEY,
                target       TEXT NOT NULL,
                base_version INTEGER NOT NULL,
                ok           INTEGER NOT NULL,
                detail       TEXT,
                created_at   REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cache_operations (
                operation_id        TEXT PRIMARY KEY,
                group_name          TEXT NOT NULL,
                target              TEXT NOT NULL,
                target_url          TEXT NOT NULL,
                route_version       INTEGER NOT NULL,
                status              TEXT NOT NULL,
                result_json         TEXT,
                last_error          TEXT,
                clear_count         INTEGER,
                verify_request_id   TEXT,
                created_at          REAL NOT NULL,
                updated_at          REAL NOT NULL,
                verified_at         REAL
            );
            CREATE INDEX IF NOT EXISTS idx_cache_ops_target
                ON cache_operations (target, status);
            """
        )
        for name, url in upstreams.items():
            _conn.execute(
                "INSERT INTO upstreams (name, url) VALUES (?, ?) "
                "ON CONFLICT(name) DO UPDATE SET url = excluded.url",
                (name, url),
            )
        seeded = False
        if _conn.execute("SELECT COUNT(*) AS c FROM routes").fetchone()["c"] == 0:
            _conn.execute(
                "INSERT INTO routes (id, current_target, version, updated_at) "
                "VALUES (1, ?, 0, ?)",
                (initial_target, time.time()),
            )
            seeded = True
        # Interrupted requests of a previous process must not linger as in-flight.
        cleared = _conn.execute("DELETE FROM inflight").rowcount
        pending_cache = _conn.execute(
            "SELECT COUNT(*) AS c FROM cache_operations WHERE status IN ('pending', 'unknown')"
        ).fetchone()["c"]
    return {
        "seeded_route": seeded,
        "cleared_inflight": cleared,
        "unresolved_cache_operations": pending_cache,
    }


def _db() -> sqlite3.Connection:
    assert _conn is not None, "db not initialised"
    return _conn


def get_route() -> dict:
    with _lock:
        row = _db().execute(
            "SELECT current_target, version, updated_at FROM routes WHERE id = 1"
        ).fetchone()
    return dict(row)


def upstream_url(name: str) -> str | None:
    with _lock:
        row = _db().execute(
            "SELECT url FROM upstreams WHERE name = ?", (name,)
        ).fetchone()
    return row["url"] if row else None


def list_upstreams() -> dict[str, str]:
    with _lock:
        rows = _db().execute("SELECT name, url FROM upstreams ORDER BY name").fetchall()
    return {r["name"]: r["url"] for r in rows}


def admit(path: str) -> dict:
    """Atomically snapshot the effective route and register the request as
    in-flight. The request keeps this target even if a switch commits while
    it is running, so it always completes against the original upstream."""
    rid = uuid.uuid4().hex
    with _lock, _db() as conn:
        route = conn.execute(
            "SELECT current_target, version FROM routes WHERE id = 1"
        ).fetchone()
        conn.execute(
            "INSERT INTO inflight (id, target, path, started_at) VALUES (?, ?, ?, ?)",
            (rid, route["current_target"], path, time.time()),
        )
    return {"id": rid, "target": route["current_target"], "version": route["version"]}


def finish(request_id: str) -> None:
    with _lock, _db() as conn:
        conn.execute("DELETE FROM inflight WHERE id = ?", (request_id,))


def record_probe(target: str, base_version: int, ok: bool, detail: dict) -> dict:
    """A probe never mutates the route; it only records an auditable result."""
    pid = uuid.uuid4().hex
    with _lock, _db() as conn:
        conn.execute(
            "INSERT INTO probes (id, target, base_version, ok, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (pid, target, base_version, 1 if ok else 0, json.dumps(detail), time.time()),
        )
    return {"id": pid}


def commit_switch(target: str, base_version: int, probe_id: str) -> dict:
    """Compare-and-swap the route on its version. A commit carrying a stale
    version (e.g. a late probe from an interleaved switch attempt) loses and
    cannot overwrite the already-effective route."""
    with _lock, _db() as conn:
        route = conn.execute(
            "SELECT current_target, version FROM routes WHERE id = 1"
        ).fetchone()
        if route["version"] != base_version:
            return {
                "ok": False,
                "error": "version_conflict",
                "current_version": route["version"],
                "current_target": route["current_target"],
            }
        if route["current_target"] == target:
            return {
                "ok": False,
                "error": "target_is_current",
                "current_version": route["version"],
                "current_target": route["current_target"],
            }
        if upstream_url(target) is None:
            return {"ok": False, "error": "unknown_target"}
        probe = conn.execute(
            "SELECT * FROM probes WHERE id = ?", (probe_id,)
        ).fetchone()
        if (
            probe is None
            or not probe["ok"]
            or probe["target"] != target
            or probe["base_version"] != base_version
        ):
            return {"ok": False, "error": "probe_invalid"}
        if time.time() - probe["created_at"] > PROBE_TTL_SECONDS:
            return {"ok": False, "error": "probe_stale"}
        cur = conn.execute(
            "UPDATE routes SET current_target = ?, version = version + 1, updated_at = ? "
            "WHERE id = 1 AND version = ?",
            (target, time.time(), base_version),
        )
        if cur.rowcount != 1:  # guarded by _lock; belt and braces
            return {"ok": False, "error": "version_conflict"}
        return {"ok": True, "current_target": target, "version": base_version + 1}


# ---------------------------------------------------------------------------
# Idempotent cache-clear operations
# ---------------------------------------------------------------------------


def _op_view(row: sqlite3.Row) -> dict:
    return {
        "operation_id": row["operation_id"],
        "group": row["group_name"],
        "target": row["target"],
        "target_url": row["target_url"],
        "route_version": row["route_version"],
        "status": row["status"],
        "result": json.loads(row["result_json"]) if row["result_json"] else None,
        "last_error": row["last_error"],
        "clear_count": row["clear_count"],
        "verify_request_id": row["verify_request_id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "verified_at": row["verified_at"],
        "resolved": row["status"] in ("done", "conflict", "not_cleared"),
    }


def _attempt_alive(conn: sqlite3.Connection, request_id: str | None) -> bool:
    """True while the leased first attempt is actually still running in this
    process. After a gateway crash/restart `inflight` is wiped, so a `pending`
    operation is treated as unknown and re-verified against the original
    target rather than waited on forever."""
    if not request_id:
        return False
    row = conn.execute(
        "SELECT 1 FROM inflight WHERE id = ?", (request_id,)
    ).fetchone()
    return row is not None


def _attempt_stale(conn: sqlite3.Connection, request_id: str | None) -> bool:
    """Defensive upper bound: a foreground attempt can never outlive the
    gateway's upstream HTTP timeout by much. If an in-flight row somehow
    lingers (e.g. a duplicated process), the operation is still re-verified
    against the original target instead of blocking forever."""
    if not request_id:
        return True
    row = conn.execute(
        "SELECT started_at FROM inflight WHERE id = ?", (request_id,)
    ).fetchone()
    if row is None:
        return True
    return time.time() - row["started_at"] > ATTEMPT_LEASE_SECONDS


def _arm_attempt(conn: sqlite3.Connection, row: sqlite3.Row) -> dict:
    """Lease a fresh attempt for an operation currently in `not_cleared`."""
    rid = uuid.uuid4().hex
    now = time.time()
    conn.execute(
        "UPDATE cache_operations SET status = 'pending', verify_request_id = ?, "
        "last_error = NULL, updated_at = ? WHERE operation_id = ?",
        (rid, now, row["operation_id"]),
    )
    conn.execute(
        "INSERT INTO inflight (id, target, path, started_at) VALUES (?, ?, ?, ?)",
        (rid, row["target"], f"/cache/clear {row['group_name']}", now),
    )
    fresh = conn.execute(
        "SELECT * FROM cache_operations WHERE operation_id = ?",
        (row["operation_id"],),
    ).fetchone()
    return {
        "outcome": "accepted",
        "operation": _op_view(fresh),
        "request_id": rid,
        "first_attempt": False,
    }


def begin_cache_operation(operation_id: str, group: str) -> dict:
    """First call for an operation id binds it to the *current* route target.

    Returns one of:
      accepted  - this caller owns the first attempt (lease request id, target)
      replay    - a resolved operation exists; return the stored result
      in_sync   - an attempt for this operation is genuinely still running in
                  this process; caller should wait and retry rather than race it
      unknown   - unresolved operation exists; caller may re-verify the
                  *original* target (never the current route)
      conflict  - operation id already bound to another group
    """
    with _lock, _db() as conn:
        row = conn.execute(
            "SELECT * FROM cache_operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        if row is not None:
            if row["group_name"] != group:
                return {
                    "outcome": "conflict",
                    "operation": _op_view(row),
                    "error": "operation_group_conflict",
                    "bound_group": row["group_name"],
                }
            view = _op_view(row)
            if view["status"] in ("done", "conflict"):
                return {"outcome": "replay", "operation": view}
            if view["status"] == "not_cleared":
                # The original upstream confirmed this operation never ran, so
                # retrying the *same* operation id is safe and still cannot
                # clear twice.
                return _arm_attempt(conn, row)
            if (
                view["status"] == "pending"
                and _attempt_alive(conn, row["verify_request_id"])
                and not _attempt_stale(conn, row["verify_request_id"])
            ):
                return {"outcome": "in_sync", "operation": view}
            return {"outcome": "unknown", "operation": view}

        route = conn.execute(
            "SELECT current_target, version FROM routes WHERE id = 1"
        ).fetchone()
        target = route["current_target"]
        url = upstream_url(target)
        if url is None:
            return {"outcome": "no_target"}
        rid = uuid.uuid4().hex
        now = time.time()
        conn.execute(
            "INSERT INTO cache_operations (operation_id, group_name, target, "
            "target_url, route_version, status, verify_request_id, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)",
            (operation_id, group, target, url, route["version"], rid, now, now),
        )
        conn.execute(
            "INSERT INTO inflight (id, target, path, started_at) VALUES (?, ?, ?, ?)",
            (rid, target, f"/cache/clear {group}", now),
        )
        created = conn.execute(
            "SELECT * FROM cache_operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
    return {
        "outcome": "accepted",
        "operation": _op_view(created),
        "request_id": rid,
        "first_attempt": True,
    }


def begin_cache_verification(operation_id: str) -> dict:
    """Reserve a verification attempt against the operation's original target.

    Used when the first result was lost (response timeout, gateway killed).
    A new request id is leased so the in-flight row / offline hint stays
    conservative until the caller records the outcome.
    """
    with _lock, _db() as conn:
        row = conn.execute(
            "SELECT * FROM cache_operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        if row is None:
            return {"outcome": "not_found"}
        view = _op_view(row)
        if view["status"] in ("done", "conflict"):
            return {"outcome": "replay", "operation": view}
        if view["status"] == "not_cleared":
            return _arm_attempt(conn, row)
        if _attempt_alive(conn, row["verify_request_id"]) and not _attempt_stale(
            conn, row["verify_request_id"]
        ):
            return {"outcome": "in_sync", "operation": view}
        rid = uuid.uuid4().hex
        now = time.time()
        conn.execute(
            "UPDATE cache_operations SET verify_request_id = ?, updated_at = ? "
            "WHERE operation_id = ?",
            (rid, now, operation_id),
        )
        conn.execute(
            "INSERT INTO inflight (id, target, path, started_at) VALUES (?, ?, ?, ?)",
            (rid, row["target"], f"/cache/verify {row['group_name']}", now),
        )
        row = conn.execute(
            "SELECT * FROM cache_operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
    return {"outcome": "reserved", "operation": _op_view(row), "request_id": rid}


def mark_cache_operation(
    operation_id: str,
    request_id: str,
    status: str,
    result: dict | None = None,
    error: str | None = None,
) -> dict | None:
    """Record an upstream outcome, but only if this caller still holds the
    leased attempt. Concurrent attempts are harmless: the first recorded
    result wins and later attempts simply observe it."""
    with _lock, _db() as conn:
        row = conn.execute(
            "SELECT * FROM cache_operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        if row is None:
            return None
        if row["status"] in ("done", "conflict", "not_cleared"):
            return _op_view(row)
        if row["verify_request_id"] != request_id:
            return _op_view(row)
        now = time.time()
        if status == "done" and result is not None:
            conn.execute(
                "UPDATE cache_operations SET status = 'done', result_json = ?, "
                "clear_count = ?, last_error = NULL, verified_at = ?, updated_at = ? "
                "WHERE operation_id = ?",
                (
                    json.dumps(result),
                    result.get("clear_count"),
                    now,
                    now,
                    operation_id,
                ),
            )
        elif status in ("conflict", "not_cleared"):
            conn.execute(
                "UPDATE cache_operations SET status = ?, last_error = ?, "
                "verified_at = ?, updated_at = ? WHERE operation_id = ?",
                (status, error or status, now, now, operation_id),
            )
        else:  # unknown
            conn.execute(
                "UPDATE cache_operations SET status = 'unknown', last_error = ?, "
                "updated_at = ? WHERE operation_id = ?",
                (error, now, operation_id),
            )
        row = conn.execute(
            "SELECT * FROM cache_operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
    return _op_view(row)


def release_inflight(request_id: str) -> None:
    finish(request_id)


def cache_operation(operation_id: str) -> dict | None:
    """Look up by exact operation id, or by unique prefix (console convenience)."""
    with _lock:
        row = _db().execute(
            "SELECT * FROM cache_operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        if row is None and operation_id:
            escaped = (
                operation_id.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            )
            rows = _db().execute(
                "SELECT * FROM cache_operations WHERE operation_id LIKE ? ESCAPE '\\' "
                "LIMIT 2",
                (escaped + "%",),
            ).fetchall()
            if len(rows) == 1:
                row = rows[0]
    return _op_view(row) if row else None


def unresolved_cache_by_target() -> dict[str, int]:
    with _lock:
        rows = _db().execute(
            "SELECT target, COUNT(*) AS c FROM cache_operations "
            "WHERE status IN ('pending', 'unknown') GROUP BY target"
        ).fetchall()
    return {r["target"]: r["c"] for r in rows}


def get_state() -> dict:
    with _lock:
        route = dict(_db().execute("SELECT * FROM routes WHERE id = 1").fetchone())
        upstreams = [
            dict(r) for r in _db().execute("SELECT name, url FROM upstreams ORDER BY name")
        ]
        inflight = [
            dict(r) for r in _db().execute("SELECT * FROM inflight ORDER BY started_at")
        ]
        probes = [
            dict(r)
            for r in _db().execute(
                "SELECT * FROM probes ORDER BY created_at DESC LIMIT 10"
            )
        ]
        cache_rows = _db().execute(
            "SELECT * FROM cache_operations ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
        unresolved = unresolved_cache_by_target()
    counts: dict[str, int] = {}
    for row in inflight:
        counts[row["target"]] = counts.get(row["target"], 0) + 1
    now = time.time()
    for u in upstreams:
        u["inflight"] = counts.get(u["name"], 0)
        u["is_current"] = u["name"] == route["current_target"]
        u["unresolved_cache_operations"] = unresolved.get(u["name"], 0)
        # A target may only be taken offline once its in-flight requests have
        # drained *and* every cache clear it was bound to has a verified
        # result. An unknown result must never show "safe to take offline".
        u["can_offline"] = (
            (not u["is_current"])
            and u["inflight"] == 0
            and u["unresolved_cache_operations"] == 0
        )
        if u["is_current"]:
            u["offline_blocked_reason"] = "current_target"
        elif u["inflight"] > 0:
            u["offline_blocked_reason"] = "inflight"
        elif u["unresolved_cache_operations"] > 0:
            u["offline_blocked_reason"] = "cache_result_unknown"
        else:
            u["offline_blocked_reason"] = None
    for row in inflight:
        row["age_ms"] = int((now - row["started_at"]) * 1000)
        row["kind"] = "cache_clear" if row["path"].startswith("/cache/") else "forward"
    for p in probes:
        p["detail"] = json.loads(p["detail"]) if p["detail"] else {}
    return {
        "now": now,
        "route": route,
        "upstreams": upstreams,
        "inflight": inflight,
        "recent_probes": probes,
        "cache_operations": [_op_view(r) for r in cache_rows],
    }
