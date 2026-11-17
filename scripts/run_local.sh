#!/usr/bin/env bash
# Local end-to-end harness: starts two upstreams + gateway (no docker needed),
# then verifies:
#   1. switch semantics + kill -9 restart recovery      (smoke_test.py)
#   2. idempotent cache clear: concurrency, lost reply,
#      switch binding, unreachable original target       (cache_test.py)
#   3. cache clear interrupted by kill -9 + restart      (cache_restart_test.py)
# Mirrors what docker-compose.yml wires up for containers.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${WORK_DIR:-/tmp/edge-gw-demo}"
SAVE="$WORK/phase1.json"
CACHE_SAVE="$WORK/cache-phase1.json"
PY="${PYTHON:-python3}"
GW_PORT="${GW_PORT:-8000}"
UP_A_PORT="${UP_A_PORT:-9001}"
UP_B_PORT="${UP_B_PORT:-9002}"
# Make a lost reply explicit in the harness: a clear whose upstream response
# takes longer than this is recorded as "unknown" and must be verified/retried
# by operation id. The default outside tests is 15s.
export CACHE_CLEAR_TIMEOUT_SECONDS="${CACHE_CLEAR_TIMEOUT_SECONDS:-3}"

rm -rf "$WORK"
mkdir -p "$WORK"

PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; }
trap cleanup EXIT

start_upstream() { # name port delay db
  UPSTREAM_NAME="$1" DEFAULT_DELAY_MS="$3" UPSTREAM_DB="$4" \
    "$PY" -m uvicorn app.main:app --app-dir "$ROOT/upstream" \
      --host 127.0.0.1 --port "$2" --log-level warning \
      >"$WORK/upstream-$1.log" 2>&1 &
  PIDS+=($!)
}

start_gateway() {
  GATEWAY_DB="$WORK/gateway.db" \
  UPSTREAMS="alpha=http://127.0.0.1:$UP_A_PORT;beta=http://127.0.0.1:$UP_B_PORT;dead=http://127.0.0.1:9" \
  INITIAL_TARGET=alpha \
    "$PY" -m uvicorn app.main:app --app-dir "$ROOT/gateway" \
      --host 127.0.0.1 --port "$GW_PORT" --log-level warning \
      >>"$WORK/gateway.log" 2>&1 &
  PIDS+=($!)
}

wait_ready() { # url
  for _ in $(seq 1 100); do
    if curl -fsS "$1" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "timeout waiting for $1" >&2
  return 1
}

echo "== 启动上游 alpha(:$UP_A_PORT, 延迟2000ms) / beta(:$UP_B_PORT, 延迟500ms) =="
start_upstream alpha "$UP_A_PORT" 2000 "$WORK/upstream-alpha.db"
start_upstream beta  "$UP_B_PORT" 500  "$WORK/upstream-beta.db"
wait_ready "http://127.0.0.1:$UP_A_PORT/health"
wait_ready "http://127.0.0.1:$UP_B_PORT/health"

echo "== 启动网关(:$GW_PORT) =="
start_gateway
wait_ready "http://127.0.0.1:$GW_PORT/api/health"

echo "== 阶段一：切换语义 =="
"$PY" "$ROOT/scripts/smoke_test.py" "http://127.0.0.1:$GW_PORT" "$SAVE" phase1 &
SMOKE=$!

# wait until phase1 has armed the to-be-interrupted in-flight request
for _ in $(seq 1 300); do [ -f "$SAVE" ] && break; sleep 0.1; done
[ -f "$SAVE" ] || { echo "phase1 未到达重启点" >&2; exit 1; }

GW_PID="${PIDS[-1]}"
echo "== kill -9 网关（模拟崩溃，beta 上仍有 1 条在途请求） =="
kill -9 "$GW_PID" 2>/dev/null || true
wait "$SMOKE"

echo "== 重启网关（同一 SQLite 数据文件） =="
start_gateway
wait_ready "http://127.0.0.1:$GW_PORT/api/health"

echo "== 阶段二：重启恢复 =="
"$PY" "$ROOT/scripts/smoke_test.py" "http://127.0.0.1:$GW_PORT" "$SAVE" phase2

echo "== 阶段三：幂等清缓存（并发 / 回包丢失 / 切换绑定 / 原目标不可达） =="
"$PY" "$ROOT/scripts/cache_test.py" \
  "http://127.0.0.1:$GW_PORT" "http://127.0.0.1:$UP_A_PORT" "http://127.0.0.1:$UP_B_PORT"

echo "== 阶段四：清缓存途中 kill -9 网关并重启 =="
"$PY" "$ROOT/scripts/cache_restart_test.py" \
  "http://127.0.0.1:$GW_PORT" "http://127.0.0.1:$UP_A_PORT" "$CACHE_SAVE" phase1 &
CACHE_P1=$!
for _ in $(seq 1 300); do [ -f "$CACHE_SAVE" ] && break; sleep 0.1; done
[ -f "$CACHE_SAVE" ] || { echo "cache phase1 未到达重启点" >&2; exit 1; }

GW_PID="${PIDS[-1]}"
kill -9 "$GW_PID" 2>/dev/null || true
wait "$CACHE_P1"

start_gateway
wait_ready "http://127.0.0.1:$GW_PORT/api/health"
"$PY" "$ROOT/scripts/cache_restart_test.py" \
  "http://127.0.0.1:$GW_PORT" "http://127.0.0.1:$UP_A_PORT" "$CACHE_SAVE" phase2

echo
echo "== 全部通过。日志目录: $WORK =="
