"""Edge gateway: proxies requests to the currently routed upstream and
exposes the operations API + web console for on-duty target switching and
idempotent cache-group clears."""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from . import db

STATIC_DIR = Path(__file__).parent / "static"
CACHE_CLEAR_TIMEOUT = float(os.environ.get("CACHE_CLEAR_TIMEOUT_SECONDS", "15"))
CACHE_VERIFY_TIMEOUT = float(os.environ.get("CACHE_VERIFY_TIMEOUT_SECONDS", "10"))

# Serialise attempts per operation id inside this gateway process. A concurrent
# duplicate therefore waits for the first attempt and then replays its result
# instead of racing another upstream call. Cross-process/concurrent duplicates
# are still deduplicated by the SQLite ledger.
_op_locks: dict[str, asyncio.Lock] = {}


def _op_lock(operation_id: str) -> asyncio.Lock:
    lock = _op_locks.get(operation_id)
    if lock is None:
        lock = _op_locks[operation_id] = asyncio.Lock()
    return lock


def parse_upstreams() -> dict[str, str]:
    raw = os.environ.get(
        "UPSTREAMS", "alpha=http://127.0.0.1:9001;beta=http://127.0.0.1:9002"
    )
    out: dict[str, str] = {}
    for part in raw.split(";"):
        part = part.strip()
        if not part:
            continue
        name, _, url = part.partition("=")
        out[name.strip()] = url.strip().rstrip("/")
    return out


UPSTREAMS = parse_upstreams()
INITIAL_TARGET = os.environ.get("INITIAL_TARGET") or next(iter(UPSTREAMS))


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.boot_info = db.init_db(UPSTREAMS, INITIAL_TARGET)
    app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.0))
    yield
    await app.state.http.aclose()


app = FastAPI(title="edge-gateway", lifespan=lifespan)


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/state")
def state():
    s = db.get_state()
    s["boot"] = app.state.boot_info
    return s


class CallRequest(BaseModel):
    delay_ms: int | None = Field(default=None, ge=0, le=60_000)
    path: str = "/work"
    group: str | None = Field(default=None, max_length=128)


@app.post("/api/call")
async def call(req: CallRequest):
    """Admit a request and proxy it to the upstream that is current *at
    admission time*. The full upstream response is fetched from that original
    target even if a switch commits while the request is in flight."""
    path = req.path if req.path.startswith("/") else "/work"
    if "://" in path or ".." in path:
        return JSONResponse(status_code=400, content={"ok": False, "error": "bad_path"})
    admission = db.admit(path)
    target_url = db.upstream_url(admission["target"])
    started = time.monotonic()
    try:
        params = {} if req.delay_ms is None else {"delay_ms": req.delay_ms}
        if req.group:
            params["group"] = req.group
        resp = await app.state.http.get(f"{target_url}{path}", params=params)
        try:
            body = resp.json()
        except ValueError:
            body = {"text": resp.text[:500]}
        return {
            "ok": resp.status_code == 200,
            "status": resp.status_code,
            "request_id": admission["id"],
            "target": admission["target"],
            "route_version": admission["version"],
            "duration_ms": int((time.monotonic() - started) * 1000),
            "upstream": body,
        }
    except httpx.HTTPError as exc:
        return JSONResponse(
            status_code=502,
            content={
                "ok": False,
                "error": f"upstream_error: {exc}",
                "request_id": admission["id"],
                "target": admission["target"],
                "route_version": admission["version"],
            },
        )
    finally:
        db.finish(admission["id"])


class ProbeRequest(BaseModel):
    target: str
    base_version: int


@app.post("/api/switch/probe")
async def probe(req: ProbeRequest):
    """Probe a candidate target. Never mutates the route: while probing, new
    requests keep flowing to the old target, and a failed probe leaves the
    original route untouched."""
    route = db.get_route()
    url = db.upstream_url(req.target)
    if url is None:
        return JSONResponse(
            status_code=404, content={"ok": False, "error": "unknown_target"}
        )
    if req.target == route["current_target"]:
        return {"ok": False, "error": "target_is_current", "current_version": route["version"]}
    ok, detail = False, {}
    try:
        health = await app.state.http.get(f"{url}/health", timeout=2.5)
        work = await app.state.http.get(
            f"{url}/work", params={"delay_ms": 0}, timeout=5.0
        )
        ok = health.status_code == 200 and work.status_code == 200
        detail = {"health_status": health.status_code, "work_status": work.status_code}
    except httpx.HTTPError as exc:
        detail = {"error": str(exc)}
    rec = db.record_probe(req.target, req.base_version, ok, detail)
    return {
        "ok": ok,
        "probe_id": rec["id"],
        "target": req.target,
        "base_version": req.base_version,
        "current_version": route["version"],
        "version_matches": route["version"] == req.base_version,
        "detail": detail,
    }


class CommitRequest(BaseModel):
    target: str
    base_version: int
    probe_id: str


@app.post("/api/switch/commit")
def commit(req: CommitRequest):
    """Confirm a switch. Version-guarded: once this commits, newly admitted
    requests immediately go to the new target without waiting for old
    in-flight requests; a stale commit is rejected with 409."""
    res = db.commit_switch(req.target, req.base_version, req.probe_id)
    if not res["ok"]:
        code = 409 if res["error"] == "version_conflict" else 400
        return JSONResponse(status_code=code, content=res)
    return res


# ---------------------------------------------------------------------------
# Idempotent cache-group clear
# ---------------------------------------------------------------------------


class CacheClearRequest(BaseModel):
    operation_id: str = Field(min_length=1, max_length=128)
    group: str = Field(min_length=1, max_length=128)
    delay_ms: int | None = Field(default=None, ge=0, le=60_000)


def _op_response(status: int, outcome: str, op: dict, **extra):
    body = {
        # ok means "the operator has a definite, successful first result".
        # unknown/in_progress/conflict/not_cleared are not success.
        "ok": outcome in ("done", "replayed"),
        "outcome": outcome,
        "operation": op,
    }
    body.update(extra)
    return JSONResponse(status_code=status, content=body)


async def _attempt_upstream_clear(
    op: dict, request_id: str, delay_ms: int | None, verify: bool
) -> dict:
    """Run one upstream attempt bound to op['target_url'] (the URL captured when
    the operation was first accepted - never the current route)."""
    ot = time.monotonic()
    if verify:
        call = app.state.http.get(
            f"{op['target_url']}/cache/clear/{op['operation_id']}",
            timeout=CACHE_VERIFY_TIMEOUT,
        )
    else:
        payload = {"operation_id": op["operation_id"], "group": op["group"]}
        if delay_ms is not None:
            payload["delay_ms"] = delay_ms
        call = app.state.http.post(
            f"{op['target_url']}/cache/clear", json=payload, timeout=CACHE_CLEAR_TIMEOUT
        )
    try:
        resp = await call
        body = resp.json()
        elapsed = int((time.monotonic() - ot) * 1000)
        if resp.status_code == 200:
            result = {
                "upstream": body.get("upstream"),
                "operation_id": body.get("operation_id"),
                "group": body.get("group"),
                "cleared": body.get("cleared"),
                "clear_count": body.get("clear_count"),
                "cleared_at": body.get("cleared_at"),
                "replayed_on_upstream": body.get("replayed"),
            }
            op = db.mark_cache_operation(
                op["operation_id"], request_id, "done", result=result
            )
            return {"state": "done", "operation": op, "duration_ms": elapsed}
        if resp.status_code == 409:
            op = db.mark_cache_operation(
                op["operation_id"],
                request_id,
                "conflict",
                error=body.get("error", "operation_group_conflict"),
            )
            return {"state": "conflict", "operation": op, "duration_ms": elapsed}
        if resp.status_code == 404:
            # Original target has no record: it never executed this operation.
            op = db.mark_cache_operation(
                op["operation_id"], request_id, "not_cleared", error="not_cleared"
            )
            return {
                "state": "not_cleared",
                "operation": op,
                "duration_ms": elapsed,
                "detail": body,
            }
        error = f"upstream_status_{resp.status_code}: {body.get('error', '')}"
        op = db.mark_cache_operation(op["operation_id"], request_id, "unknown", error=error)
        return {"state": "unknown", "operation": op, "error": error, "duration_ms": elapsed}
    except httpx.HTTPError as exc:
        op = db.mark_cache_operation(
            op["operation_id"], request_id, "unknown", error=f"upstream_error: {exc}"
        )
        return {
            "state": "unknown",
            "operation": op,
            "error": f"upstream_error: {exc}",
            "duration_ms": int((time.monotonic() - ot) * 1000),
        }
    finally:
        db.release_inflight(request_id)


def _load_operation(op_ref: str) -> dict | None:
    return db.cache_operation(op_ref)


@app.post("/api/cache/clear")
async def cache_clear(req: CacheClearRequest):
    """Idempotent, operation-id-keyed cache-group clear.

    * First call binds the operation to the current route target *at that
      moment* and persists that binding before calling upstream.
    * Same id + same group on retry (concurrent, after a lost response, or
      after a gateway restart) replays the first result; upstream is only
      asked again when the previous outcome is genuinely unknown or verified
      not-cleared.
    * Same id + different group is rejected with 409.
    * A switch does not re-point an existing operation: it always checks the
      target captured at first acceptance.
    """
    op_ref = req.operation_id.strip()
    group = req.group.strip()
    # Same-process concurrent duplicates queue here, then replay the first
    # result. Attempts are always keyed by the operation's stored target_url,
    # never by the current route.
    async with _op_lock(op_ref):
        decision = db.begin_cache_operation(op_ref, group)
        outcome = decision["outcome"]

        if outcome == "conflict":
            op = decision["operation"]
            return _op_response(
                409,
                "conflict",
                op,
                error="operation_group_conflict",
                response={
                    "operation_id": op_ref,
                    "group": group,
                    "bound_group": decision["bound_group"],
                },
                message=f"操作号 {op_ref} 已绑定缓存组 {decision['bound_group']}，不可改组",
            )

        if outcome == "replay":
            op = decision["operation"]
            return _op_response(
                200,
                "replayed",
                op,
                result=op["result"],
                target=op["target"],
                replayed=True,
                message=f"重放首次结果：{op['target']} 上组 {op['group']} 清理次数 {op['clear_count']}",
            )

        if outcome == "in_sync":
            op = decision["operation"]
            return _op_response(
                202,
                "in_progress",
                op,
                target=op["target"],
                retry_after_ms=500,
                message="该操作号的原尝试仍在处理中，请稍后按同一操作号重试",
            )

        if outcome == "unknown":
            op = decision["operation"]
            reserved = db.begin_cache_verification(op_ref)
            if reserved["outcome"] == "in_sync":
                op = reserved["operation"]
                return _op_response(
                    202,
                    "in_progress",
                    op,
                    target=op["target"],
                    retry_after_ms=500,
                    message="该操作号正在向原目标核实中，请稍后按同一操作号查询",
                )
            if reserved["outcome"] == "replay":
                op = reserved["operation"]
                return _op_response(
                    200,
                    "replayed",
                    op,
                    result=op["result"],
                    target=op["target"],
                    replayed=True,
                )
            op = reserved["operation"]
            request_id = reserved["request_id"]
            result = await _attempt_upstream_clear(op, request_id, req.delay_ms, verify=True)
            if result["state"] == "not_cleared":
                # The original target confirmed this operation id never ran,
                # so executing it now is still exactly-once. A retry after a
                # lost reply therefore resolves in one call.
                retry = db.begin_cache_operation(op_ref, group)
                if retry["outcome"] == "accepted":
                    result = await _attempt_upstream_clear(
                        retry["operation"], retry["request_id"], req.delay_ms, verify=False
                    )
            return _result_response(result, op_ref, group)

        if outcome == "no_target":
            return JSONResponse(
                status_code=503, content={"ok": False, "error": "no_current_target"}
            )

        # accepted: this caller owns the first attempt for this operation id.
        op = decision["operation"]
        request_id = decision["request_id"]
        result = await _attempt_upstream_clear(op, request_id, req.delay_ms, verify=False)
        return _result_response(result, op_ref, group)


def _result_response(result: dict, op_ref: str, group: str):
    op = result["operation"]
    state = result["state"]
    if state == "done":
        return _op_response(
            200,
            "done",
            op,
            target=op["target"],
            result=op["result"],
            replayed=False,
            duration_ms=result.get("duration_ms"),
            message=f"{op['target']} 已清理组 {op['group']}，该组清理次数 {op['clear_count']}",
        )
    if state == "conflict":
        return _op_response(
            409,
            "conflict",
            op,
            target=op["target"],
            error="operation_group_conflict",
            response={"operation_id": op_ref, "group": group},
            message=f"操作号 {op_ref} 已绑定其他缓存组，不可改组",
        )
    if state == "not_cleared":
        return _op_response(
            200,
            "not_cleared",
            op,
            target=op["target"],
            result=op["result"],
            retryable=True,
            message="原目标确认该操作号从未执行，未发生清理；可用同一操作号安全重试",
        )
    # unknown: the request may or may not have taken effect upstream. Never
    # suggest taking this target offline until it is verified.
    return _op_response(
        202,
        "unknown",
        op,
        target=op["target"],
        target_url=op["target_url"],
        error=result.get("error"),
        result=op["result"],
        retryable=True,
        message=(
            f"结果未定：{op['target']} 暂不可达或回包丢失，暂勿下线该目标；"
            "恢复后请用同一操作号查询/重试核实"
        ),
    )


@app.get("/api/cache/operations/{operation_id}")
def cache_operation(operation_id: str):
    op = _load_operation(operation_id)
    if op is None:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": "unknown_operation", "operation_id": operation_id},
        )
    body = {"ok": op["resolved"], "operation": op}
    if not op["resolved"]:
        body["message"] = "结果未定：原目标未核实，暂勿下线该目标"
    return body


@app.post("/api/cache/verify/{operation_id}")
async def cache_verify(operation_id: str):
    """Actively verify an unresolved operation against its original target.

    Safe after recovery: if the upstream did execute it, the stored first
    result is returned; if not, the operation is marked not_cleared and may be
    retried with the same id. The current route is never consulted."""
    op = _load_operation(operation_id)
    if op is None:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": "unknown_operation", "operation_id": operation_id},
        )
    if op["resolved"]:
        return _op_response(200, "replayed", op, result=op["result"], target=op["target"], replayed=True)
    reserved = db.begin_cache_verification(op["operation_id"])
    if reserved["outcome"] == "in_sync":
        op2 = reserved["operation"]
        return _op_response(
            202,
            "in_progress",
            op2,
            target=op2["target"],
            retry_after_ms=500,
            message="该操作号正在向原目标核实中，请稍后按同一操作号查询",
        )
    if reserved["outcome"] == "replay":
        op2 = reserved["operation"]
        return _op_response(200, "replayed", op2, result=op2["result"], target=op2["target"], replayed=True)
    op2 = reserved["operation"]
    result = await _attempt_upstream_clear(op2, reserved["request_id"], None, verify=True)
    if result["state"] == "not_cleared":
        # Verification confirms it never ran; do not silently execute the
        # clear here - the operator decides by retrying the operation id.
        return _result_response(result, op2["operation_id"], op2["group"])
    return _result_response(result, op2["operation_id"], op2["group"])


@app.get("/api/cache/groups")
async def cache_groups(target: str | None = None):
    """Demo observability: per-group clear counts read directly from an upstream."""
    route = db.get_route()
    name = target or route["current_target"]
    url = db.upstream_url(name)
    if url is None:
        return JSONResponse(status_code=404, content={"ok": False, "error": "unknown_target"})
    try:
        r = await app.state.http.get(f"{url}/cache/groups", timeout=5.0)
        body = r.json()
        return {"ok": r.status_code == 200, "target": name, "groups": body.get("groups", {})}
    except httpx.HTTPError as exc:
        return JSONResponse(
            status_code=502,
            content={"ok": False, "target": name, "error": f"upstream_error: {exc}"},
        )
