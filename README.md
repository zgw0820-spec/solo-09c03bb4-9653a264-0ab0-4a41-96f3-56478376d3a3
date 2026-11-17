# 边缘网关值班切换 Demo

FastAPI + SQLite 实现的边缘网关与操作网页。值班员在网页上切换上游、发起转发请求，
并让**当前上游**按缓存组执行一次带操作号的清缓存；演示上游在 `/work` 返回该组的清理次数。
原有转发与切换语义保持不变。

## 组成

| 组件 | 说明 |
| --- | --- |
| `gateway/` | FastAPI 网关：转发请求、切换与清缓存操作 API、值班台网页（`/`）、SQLite 持久化 |
| `upstream/` | 演示上游：可设默认响应延迟（`DEFAULT_DELAY_MS`），`/work` 返回组清理次数，幂等清缓存接口 |
| `docker-compose.yml` | 一键启动网关 + 两个本地上游（alpha 默认 3s、beta 默认 0.5s），网页在网关 `/` |
| `scripts/` | 本地（无 Docker）端到端验证脚本 |

## 运行

Docker Compose：

```bash
docker compose up --build
# 值班台网页: http://localhost:8080/
```

本地直接跑（无需 Docker，含自动化断言与两次重启演练）：

```bash
./scripts/run_local.sh
```

## 语义与实现要点

### 转发与切换（原有）

- **已接纳请求走原上游**：`/api/call` 在接纳时原子地快照当前路由并登记在途
  （`db.admit`），随后无论路由如何切换，都从快照目标取回完整响应。
- **先探测后确认**：`POST /api/switch/probe` 只探测候选（`/health` + `/work`），
  从不改写路由；`POST /api/switch/commit` 才把路由切到新目标。
- **立即生效**：确认切换只做一次版本守卫的单行 `UPDATE`，不等待旧请求结束。
- **版号防交错**：确认切换按 `version = base_version` 做 CAS，迟到的确认收到
  `409 version_conflict`，不得覆盖已生效路由。
- **重启恢复**：路由与上游登记持久化在 SQLite 中；启动时清空 `inflight` 表。

### 带操作号的幂等清缓存（新增）

值班员在网页输入**操作号** + 缓存组，点击“清除缓存组”。网关把请求发给
**首次受理时**的当前上游；演示上游按操作号记账，保证**同号同组只清一次**。

- **首次提交绑定当时的上游**：`cache_operations` 行在调用上游之前写入 SQLite，
  保存 `operation_id / group / target / target_url / route_version`。之后即便切换
  目标，该操作也只向这份快照里的 `target_url` 核对，绝不改指向新目标。
- **同号同组只清一次**：上游在上游侧 SQLite 中原子地“查操作号 → 写结果 → 计数 +1”，
  后续同号请求直接返回首次结果（`replayed: true`），清理次数不变。
  并发重试在网关进程内按操作号串行（asyncio 锁），进程间/重启后再由 SQLite 账本兜底。
- **回包丢失**：网关等待上游超时（默认 `CACHE_CLEAR_TIMEOUT_SECONDS=15`）或连接中断时，
  操作记为 `unknown`，响应 `202` + `outcome: unknown`“结果未定”。上游账本在延迟回包
  **之前**已落盘，所以重试同号会向原目标查询并返回首次结果，不会二次清理。
- **同号改组必须拒绝**：同一操作号换一个缓存组返回 `409 operation_group_conflict`，
  上游与网关都拒绝，不产生任何清理。
- **网关重启**：清缓存账本不清空。重启后操作仍绑定原目标；若首个尝试被 `kill -9`
  打断，启动信息里的 `unresolved_cache_operations` 会提示待核实数量。
- **原目标暂不可达**：操作保持 `unknown`，状态接口给该上游
  `can_offline: false`、`offline_blocked_reason: "cache_result_unknown"`，网页显示
  “不得下线 · 清缓存结果未定”。未核实前**绝不**提示可下线；恢复后用同一操作号
  重试（或调 `/api/cache/verify/{operation_id}`）即可继续核对。
- **确认未清理**：若原目标明确没有该操作号记录（404），网关记为 `not_cleared`，
  可用同一操作号安全补做一次（仍只清一次）。

## 操作 API

| 方法/路径 | 说明 |
| --- | --- |
| `GET /api/state` | 当前路由（目标+版号）、上游在途数/待核实数/可否下线、在途明细、最近探测、最近清缓存操作 |
| `POST /api/call` | 发起转发请求 `{delay_ms?, path?, group?}`，`group` 让 `/work` 返回该组清理次数 |
| `POST /api/switch/probe` | 探测候选 `{target, base_version}`，不改路由 |
| `POST /api/switch/commit` | 确认切换 `{target, base_version, probe_id}`，版号冲突返回 409 |
| `POST /api/cache/clear` | 幂等清缓存 `{operation_id, group, delay_ms?}`。结果 `outcome`: `done` / `replayed` / `unknown` / `in_progress` / `not_cleared` / `conflict` |
| `GET /api/cache/operations/{operation_id}` | 查询操作状态与首次结果（支持唯一前缀） |
| `POST /api/cache/verify/{operation_id}` | 向**原目标**核实待定操作；已核实则重放首次结果 |
| `GET /api/cache/groups?target=` | 演示观测：直接读取某个上游各组的清理次数 |

上游接口：

| 方法/路径 | 说明 |
| --- | --- |
| `POST /cache/clear` | `{operation_id, group, delay_ms?}`，幂等清理；重复返回 `replayed:true`；同号改组 409 |
| `GET /cache/clear/{operation_id}` | 查询上游账本中的首次结果；未知返回 404 |
| `GET /cache/groups` | 各组清理次数 |
| `GET /work?group=` | 原转发接口；带 `group` 时返回该组 `clear_count` |
| `POST /admin/cache_mode` | 演示钩子：让清缓存接口暂时不可用/恢复（验证“结果未定”路径） |

网页顶部固定展示当前目标与版号；上游表格新增“待定清缓存”列，未核实操作存在时
该行标红并显示“不得下线 · 清缓存结果未定”，在途清零也不会变成“可下线 ✓”。

## 验证

`scripts/run_local.sh` 启动两个上游与网关，依次执行：

1. `smoke_test.py` 两阶段（切换语义、探测失败、交错切换、慢请求原路返回、
   `kill -9` 后重启清理与路由保持）。
2. `cache_test.py`：8 路并发同号同组只清一次、同号改组 409、回包丢失（上游延迟
   回包超过网关超时）后重试重放首次结果、显式核实、空白归一化、切换前后操作绑定、
   旧操作仍向原目标核对、原目标不可达时的“结果未定 / 不得下线 / 恢复后核实”。
3. `cache_restart_test.py` 两阶段：清缓存请求在途时 `kill -9` 网关，重启后账本仍在、
   目标仍显示不可下线，同号重试核实到上游只清了一次并恢复可下线。

共 99 项断言。2026-11-17 实际运行 `./scripts/run_local.sh` 全部通过（exit 0）。
