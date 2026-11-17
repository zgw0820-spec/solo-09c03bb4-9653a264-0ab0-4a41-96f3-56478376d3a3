#!/usr/bin/env python3
"""End-to-end verification for idempotent cache-group clears.

Assumes the harness (scripts/run_local.sh) has started:
  upstream alpha at ALPHA_URL, beta at BETA_URL, gateway at BASE.

Usage: cache_test.py BASE_URL ALPHA_URL BETA_URL
"""
import json
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
ALPHA = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:9001"
BETA = sys.argv[3] if len(sys.argv) > 3 else "http://127.0.0.1:9002"

FAILURES: list[str] = []
UID = str(int(time.time() * 1000))


def req(url, method="GET", body=None, timeout=90):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(
        url, method=method, data=data, headers={"content-type": "application/json"}
    )
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, {"raw": raw.decode(errors="replace")}


def gw(path, method="GET", body=None, timeout=90):
    return req(BASE + path, method, body, timeout)


def up(url, path, method="GET", body=None, timeout=90):
    return req(url + path, method, body, timeout)


def turl_of(name):
    return ALPHA if name == "alpha" else BETA


def current_target():
    return state()["route"]["current_target"]


def other_target():
    return "beta" if current_target() == "alpha" else "alpha"


def check(name, cond, extra=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  ({extra})" if extra and not cond else ""))
    if not cond:
        FAILURES.append(name)


def group_count(url, group):
    s, b = up(url, f"/work?delay_ms=0&group={group}")
    return b.get("clear_count")


def state():
    return gw("/api/state")[1]


def upstream(name, st=None):
    st = st or state()
    return next(u for u in st["upstreams"] if u["name"] == name)


def clear(op, group, delay=None, base=BASE):
    body = {"operation_id": op, "group": group}
    if delay is not None:
        body["delay_ms"] = delay
    return req(base + "/api/cache/clear", "POST", body)


def switch_to(target, base_version):
    s, p = gw("/api/switch/probe", "POST", {"target": target, "base_version": base_version})
    assert p.get("ok"), p
    s, c = gw("/api/switch/commit", "POST",
              {"target": target, "base_version": base_version, "probe_id": p["probe_id"]})
    assert c.get("ok"), c
    return c


def test_concurrent_same_op():
    print("\n== 并发同号同组：上游只清一次 ==")
    g = f"conc-{UID}"
    op = f"op-conc-{UID}"
    target = current_target()
    results = []
    lock = threading.Lock()

    def worker():
        r = clear(op, g)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=90)
    dt = time.time() - t0
    outcomes = [r[1].get("outcome") for r in results]
    check("并发 8 次同号同组全部返回 200", all(r[0] == 200 for r in results), str([r[0] for r in results]))
    check("恰好一次首次清理，其余为重放",
          outcomes.count("done") == 1 and outcomes.count("replayed") == 7, str(outcomes))
    counts = {r[1]["operation"]["clear_count"] for r in results}
    check("所有响应报告相同清理次数 1", counts == {1}, str(counts))
    check("所有响应绑定同一目标", {r[1]["operation"]["target"] for r in results} == {target},
          str({r[1]["operation"]["target"] for r in results}))
    other = "beta" if target == "alpha" else "alpha"
    check("原目标组清理次数实际为 1", group_count(turl_of(target), g) == 1,
          f"target={target}")
    check("另一上游未清理该组", group_count(turl_of(other), g) == 0)
    print(f"   （8 次并发总耗时 {dt:.2f}s）")
    return op, g


def test_group_conflict(op, g):
    print("\n== 同号改组：必须拒绝 ==")
    s, b = clear(op, g + "-other")
    check("同号改组返回 409", s == 409 and b.get("error") == "operation_group_conflict", str(b))
    target_url = turl_of(current_target())
    check("拒绝响应不产生新清理", group_count(target_url, g) == 1)
    st = state()
    bound = next(o for o in st["cache_operations"] if o["operation_id"] == op)
    check("网关仍记录原组", bound["group"] == g and bound["status"] == "done", str(bound))


def test_lost_response():
    print("\n== 回包丢失（上游延迟回包 > 网关超时）：重试返回首次结果且不重复清理 ==")
    g = f"lost-{UID}"
    op = f"op-lost-{UID}"
    # First attempt: upstream commits the clear, then holds the response past
    # the gateway's upstream timeout so the gateway records "unknown".
    s, b = clear(op, g, delay=6000)
    check("回包丢失后网关返回结果未定(202 unknown)",
          s == 202 and b.get("outcome") == "unknown", str(b)[:400])
    target = current_target()
    turl = turl_of(target)
    st = state()
    a = upstream(target)
    check("结果未定时原目标待核实操作数为 1", a["unresolved_cache_operations"] == 1, str(a))
    check("结果未定时原目标不提示可下线", a["can_offline"] is False, str(a))
    counted = group_count(turl, g)
    check("尽管回包丢失，上游实际已清理一次", counted == 1, f"count={counted}")

    # Retry with the same operation id: gateway re-verifies against the
    # original target and replays the first result without clearing again.
    s, b = clear(op, g)
    check("同号重试核实后返回 200 replayed",
          s == 200 and b.get("outcome") in ("replayed", "done"), str(b)[:400])
    check("重试响应清理次数为 1", b.get("operation", {}).get("clear_count") == 1, str(b)[:400])
    check("上游仍只清理一次", group_count(turl, g) == 1, f"count={group_count(turl, g)}")
    a = upstream(target)
    check("核实后原目标待核实数清零", a["unresolved_cache_operations"] == 0, str(a))
    s, q = gw(f"/api/cache/operations/{op}")
    check("查询该操作显示已核实", s == 200 and q["operation"]["status"] == "done", str(q)[:300])


def test_verify_endpoint():
    print("\n== 显式“核实待定”接口：恢复后核实并可安全补做一次 ==")
    target = current_target()
    turl = turl_of(target)
    g = f"verify-{UID}"
    op = f"op-verify-{UID}"
    up(turl, "/admin/cache_mode", "POST", {"fail": True})
    try:
        s, b = clear(op, g)
        check("不可达时进入结果未定", s == 202 and b.get("outcome") == "unknown", str(b)[:300])
    finally:
        up(turl, "/admin/cache_mode", "POST", {"fail": False})
    s, q = gw(f"/api/cache/verify/{op}", "POST")
    check("恢复后核实确认原目标从未执行（not_cleared）",
          s == 200 and q.get("outcome") == "not_cleared"
          and q["operation"]["target"] == target, str(q)[:400])
    check("只核实不会替值班员执行清理", group_count(turl, g) == 0)
    # Now the operator explicitly retries with the same operation id: it is
    # safe because the original target confirmed it never ran.
    s, q = clear(op, g)
    check("确认未清理后用同号补做得到已清理结果",
          s == 200 and q.get("outcome") == "done"
          and q["operation"]["clear_count"] == 1, str(q)[:400])
    check("补做后上游该组清理次数为 1", group_count(turl, g) == 1)
    s, q = gw(f"/api/cache/verify/{op}", "POST")
    check("已核实操作再次核实直接重放首次结果",
          s == 200 and q.get("outcome") == "replayed"
          and q["operation"]["clear_count"] == 1, str(q)[:300])
    check("重复核实未产生第二次清理", group_count(turl, g) == 1)


def test_same_op_group_case():
    print("\n== 操作号大小写/空白归一（同号）与不同操作号互不影响 ==")
    target = current_target()
    turl = turl_of(target)
    base = f"op-norm-{UID}"
    g1 = f"norm-a-{UID}"
    s1, b1 = clear(base, g1)
    s2, b2 = clear("  " + base + "  ", g1)
    check("带空白同号同组视为重放", s1 == 200 and b1["outcome"] == "done"
          and s2 == 200 and b2["outcome"] == "replayed", str(b2)[:300])
    other_op = base + "-2"
    g2 = f"norm-b-{UID}"
    s3, b3 = clear(other_op, g2)
    check("不同操作号可清理另一组", s3 == 200 and b3["outcome"] == "done"
          and b3["operation"]["clear_count"] == 1, str(b3)[:300])
    check("两组清理互不串扰", group_count(turl, g1) == 1 and group_count(turl, g2) == 1)


def test_switch_binding():
    print("\n== 切换目标：首次提交绑定当时上游，旧操作仍向原目标核对 ==")
    old_target = current_target()
    new_target = other_target()
    old_url = turl_of(old_target)
    new_url = turl_of(new_target)
    # An operation accepted before the switch is bound to the then-current
    # target and its first attempt loses the response -> unresolved.
    g_old = f"oldtarget-{UID}"
    op_old = f"op-old-{UID}"
    s, b = clear(op_old, g_old, delay=6000)
    check(f"切换前受理的操作绑定当时目标 {old_target} 并进入未定",
          b["operation"]["target"] == old_target and b["outcome"] == "unknown", str(b)[:300])

    v = state()["route"]["version"]
    c = switch_to(new_target, v)
    check(f"已切换到 {new_target}", c.get("current_target") == new_target, str(c))

    st = state()
    a = upstream(old_target, st)
    check(f"旧目标 {old_target} 有未核实操作时不得提示可下线",
          a["can_offline"] is False, str(a))
    check("原因标为 cache_result_unknown",
          a["offline_blocked_reason"] == "cache_result_unknown", str(a))

    g_new = f"newtarget-{UID}"
    op_new = f"op-new-{UID}"
    s, b = clear(op_new, g_new)
    check(f"切换后新操作走新目标 {new_target}",
          s == 200 and b["operation"]["target"] == new_target, str(b)[:300])
    check(f"新目标 {new_target} 上清理次数为 1", group_count(new_url, g_new) == 1)
    check(f"旧目标 {old_target} 未收到新操作", group_count(old_url, g_new) == 0)

    # The old operation must verify against its original target even though
    # the route has moved on.
    s, b = clear(op_old, g_old)
    check(f"旧操作核实仍向原目标 {old_target} 查询，返回首次结果",
          s == 200 and b["operation"]["target"] == old_target
          and b["operation"]["clear_count"] == 1, str(b)[:300])
    check(f"旧目标 {old_target} 上该组确实只清理一次", group_count(old_url, g_old) == 1)
    check(f"新目标 {new_target} 上该组从未清理", group_count(new_url, g_old) == 0)
    st = state()
    check(f"核实后旧目标 {old_target} 恢复可下线",
          upstream(old_target, st)["can_offline"] is True, str(upstream(old_target, st)))


def test_unreachable_then_recover():
    print("\n== 原目标暂不可达：显示结果未定，未核实不得下线；恢复后可继续查询 ==")
    target = current_target()
    turl = turl_of(target)
    g = f"down-{UID}"
    op = f"op-down-{UID}"
    up(turl, "/admin/cache_mode", "POST", {"fail": True})
    try:
        s, b = clear(op, g)
        check("原目标不可达时返回结果未定",
              s == 202 and b.get("outcome") == "unknown", str(b)[:300])
        check("结果未定响应明确提示暂勿下线",
              "勿下线" in (b.get("message") or ""), str(b.get("message")))
        _, q = gw(f"/api/cache/operations/{op}")
        check("查询显示结果未定", q["operation"]["status"] == "unknown", str(q)[:300])
        check("未核实期间查询不给出可下线结论", q["ok"] is False, str(q))
        check("未核实期间原目标未发生清理", group_count(turl, g) == 0)

        # Switch away while the operation is still unresolved: the old target
        # must be marked not-offlinable with the cache reason.
        v = state()["route"]["version"]
        switch_to(other_target(), v)
        a = upstream(target)
        check(f"未核实前不得提示旧目标 {target} 可下线", a["can_offline"] is False, str(a))
        check("不可下线原因标为 cache_result_unknown",
              a["offline_blocked_reason"] == "cache_result_unknown", str(a))
    finally:
        up(turl, "/admin/cache_mode", "POST", {"fail": False})

    # Recovery: gateway must verify against the original target without
    # re-clearing; it never ran, so it may clear exactly once now.
    s, b = clear(op, g)
    check("恢复后同号重试得到确定结果",
          s in (200, 202) and b.get("outcome") in ("done", "not_cleared", "unknown"),
          str(b)[:400])
    if b.get("outcome") in ("not_cleared", "unknown"):
        s, b = clear(op, g)
    check("恢复后重试得到已清理结果", b.get("outcome") == "done" and b["operation"]["clear_count"] == 1,
          str(b)[:400])
    check("恢复后上游该组清理次数为 1", group_count(turl, g) == 1,
          f"count={group_count(turl, g)}")
    a = upstream(target)
    check(f"恢复核实后 {target} 可下线恢复", a["can_offline"] is True, str(a))
    s, q = gw(f"/api/cache/operations/{op}")
    check("恢复后查询返回已核实结果", s == 200 and q["operation"]["status"] == "done", str(q)[:300])


def test_query_counts_visible():
    print("\n== 演示上游在 /work 返回该组清理次数 ==")
    g = f"visible-{UID}"
    op = f"op-vis-{UID}"
    s, b = clear(op, g)
    check("清理成功", s == 200 and b["outcome"] == "done", str(b)[:300])
    s, r = gw("/api/call", "POST", {"delay_ms": 0, "group": g})
    cur = current_target()
    check("网关转发 /work 返回到该组清理次数 1",
          r.get("upstream", {}).get("clear_count") == 1, str(r)[:400])
    check("转发仍绑定当前目标", r.get("target") == cur, f"{r.get('target')} != {cur}")


if __name__ == "__main__":
    test_query_counts_visible()
    op, g = test_concurrent_same_op()
    test_group_conflict(op, g)
    test_lost_response()
    test_verify_endpoint()
    test_same_op_group_case()
    test_switch_binding()
    test_unreachable_then_recover()
    st = state()
    if st["route"]["current_target"] != "alpha":
        switch_to("alpha", st["route"]["version"])
    if FAILURES:
        print(f"\n{len(FAILURES)} 项检查失败:")
        for f in FAILURES:
            print(" -", f)
        sys.exit(1)
    print("\n清缓存全部检查通过")
