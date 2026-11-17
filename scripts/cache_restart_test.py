#!/usr/bin/env python3
"""Gateway-restart scenario for an in-flight idempotent cache clear.

phase1: start a clear against alpha with a long upstream response delay, wait
        until it is registered as pending/in-flight, persist the operation id,
        then keep polling until the harness kills the gateway.
phase2: after restart, the operation must still be bound to alpha and
        unresolved; alpha must not be shown as offlinable; retrying the same
        operation id must verify against alpha and return the first result
        without a second clear.

Usage: cache_restart_test.py BASE_URL ALPHA_URL SAVE_FILE phase1|phase2
"""
import json
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = sys.argv[1]
ALPHA = sys.argv[2]
SAVE = sys.argv[3]
PHASE = sys.argv[4] if len(sys.argv) > 4 else "phase1"

FAILURES: list[str] = []
UID = str(int(time.time() * 1000))


def req(url, method="GET", body=None, timeout=60):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(
        url, method=method, data=data, headers={"content-type": "application/json"}
    )
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def gw(path, method="GET", body=None, timeout=60):
    return req(BASE + path, method, body, timeout)


def check(name, cond, extra=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  ({extra})" if extra and not cond else ""))
    if not cond:
        FAILURES.append(name)


def state():
    return gw("/api/state")[1]


def upd(url, path, method="GET", body=None):
    return req(url + path, method, body)


def group_count(url, group):
    return upd(url, f"/work?delay_ms=0&group={group}")[1].get("clear_count")


def switch_to(target, base_version):
    s, p = gw("/api/switch/probe", "POST", {"target": target, "base_version": base_version})
    assert p.get("ok"), p
    s, c = gw("/api/switch/commit", "POST",
              {"target": target, "base_version": base_version, "probe_id": p["probe_id"]})
    assert c.get("ok"), c


def phase1():
    g = f"restart-{UID}"
    op = f"op-restart-{UID}"
    with open(SAVE + ".pending", "w") as f:
        json.dump({"operation_id": op, "group": g, "target": "alpha", "url": ALPHA}, f)

    def fire():
        try:
            req(BASE + "/api/cache/clear", "POST",
                {"operation_id": op, "group": g, "delay_ms": 20000}, timeout=60)
        except Exception:  # noqa: BLE001 - gateway being killed
            pass

    threading.Thread(target=fire, daemon=True).start()

    deadline = time.time() + 15
    armed = False
    while time.time() < deadline:
        try:
            st = state()
            ops = [o for o in st["cache_operations"] if o["operation_id"] == op]
            if (
                ops
                and ops[0]["status"] == "pending"
                and ops[0]["target"] == "alpha"
                and any(x["path"].startswith("/cache/clear") for x in st["inflight"])
            ):
                armed = True
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.1)
    check("清缓存在途中（pending + 在途记录）", armed)
    if armed:
        # The upstream commits the ledger before holding the response, so by
        # the time the gateway is killed the clear has already taken effect.
        check("上游已实际清理一次（回包未返回）", group_count(ALPHA, g) == 1)
        with open(SAVE, "w") as f:
            json.dump({"operation_id": op, "group": g, "target": "alpha", "url": ALPHA}, f)
        print("PHASE1 READY: 清缓存请求挂起，等待网关被杀")

    # Keep alive until the harness kills the gateway.
    while time.time() < deadline + 60:
        try:
            gw("/api/state", timeout=2)
            time.sleep(0.2)
        except Exception:  # noqa: BLE001
            print("PHASE1 OK: 网关已停止")
            return
    check("等待网关被杀", False)


def phase2():
    with open(SAVE) as f:
        saved = json.load(f)
    op, g, alpha = saved["operation_id"], saved["group"], saved["url"]
    st = state()
    check("重启后路由仍为 alpha", st["route"]["current_target"] == "alpha", str(st["route"]))
    check("重启后清缓存操作仍在账本中（未丢失）",
          any(o["operation_id"] == op for o in st["cache_operations"]))
    check("重启后启动信息报告待核实操作",
          st["boot"].get("unresolved_cache_operations", 0) >= 1, str(st["boot"]))

    s, q = gw(f"/api/cache/operations/{op}")
    check("重启后查询显示结果未定", s == 200 and q["operation"]["status"] in ("pending", "unknown")
          and q["operation"]["target"] == "alpha", str(q)[:300])
    check("待定查询不给出可下线结论", q["ok"] is False)

    v = st["route"]["version"]
    switch_to("beta", v)
    st = state()
    a = next(u for u in st["upstreams"] if u["name"] == "alpha")
    check("未核实前 alpha 不得提示可下线", a["can_offline"] is False, str(a))
    check("不可下线原因标为 cache_result_unknown",
          a["offline_blocked_reason"] == "cache_result_unknown", str(a))

    # Same operation id after restart: verify against the *original* target.
    s, b = gw("/api/cache/clear", "POST", {"operation_id": op, "group": g})
    check("重启后同号重试返回首次结果",
          s == 200 and b.get("outcome") in ("replayed", "done")
          and b["operation"]["target"] == "alpha"
          and b["operation"]["clear_count"] == 1, str(b)[:400])
    check("重启重试未导致第二次清理", group_count(alpha, g) == 1,
          f"count={group_count(alpha, g)}")
    a = next(u for u in state()["upstreams"] if u["name"] == "alpha")
    check("核实后 alpha 待核实数清零", a["unresolved_cache_operations"] == 0, str(a))
    check("核实后 alpha 可下线", a["can_offline"] is True, str(a))

    # restore route
    st = state()
    switch_to("alpha", st["route"]["version"])


if __name__ == "__main__":
    if PHASE == "phase1":
        phase1()
    else:
        phase2()
    if FAILURES:
        print(f"\n{len(FAILURES)} 项检查失败:")
        for f in FAILURES:
            print(" -", f)
        sys.exit(1)
    print(f"\n全部检查通过 (cache {PHASE})")
