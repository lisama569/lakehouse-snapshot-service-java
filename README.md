# 本地湖仓表增量写入与快照管理服务（Lakehouse Snapshot Service）

一个**不依赖任何外部服务**（无数据库、无 HDFS、无云存储）的本地湖仓能力实现，基于
Spring Boot 3 + Java 21，数据全部落在本地文件系统。用于验证：

1. 按批次写入 / 按主键 upsert 的**幂等语义**与可区分的拒绝原因；
2. 新增 / 重命名 / 删除字段后的**模式演进与读兼容**；
3. **不可变快照、时间旅行**与写入/读取/合并/清理并发下的一致性。

---

## 1. 目录布局与存储模型

所有数据位于 `lakehouse.data-dir`（默认 `build/lakehouse-data`）：

```
{data-dir}/{table}/
├── state/
│   ├── state.json        # 表元数据（模式、快照链、批次回执），原子覆盖写
│   └── commit.lock       # 跨进程提交锁文件
├── data/
│   └── snap-{id}-{uuid}.jsonl   # 不可变数据文件（JSON Lines，按列 id 存储）
└── staging/              # 临时文件目录，崩溃残留仅在持锁临界区内回收
```

- **数据文件不可变**：提交后永不修改、原地重写只发生在 UPSERT/COMPACT 生成的*新*文件上。
- 每行 JSON 形如 `{"v":3,"c":{"1":"alice","2":17}}`：`v` 是写入时模式版本，
  `c` 的键是**列 id**（不是列名），这是列重命名后旧数据不错列的关键。
- **state.json 采用“临时文件 + ATOMIC_MOVE”覆盖写**，任何时刻读到的状态要么是旧版本、
  要么是新版本，不存在半成品 JSON。

---

## 2. 写入语义

### 2.1 批次与位点（offset）

- `committedOffset` = 已成功提交的批次数，从 0 开始，每次成功提交严格 +1。
- 每个写入请求必须携带 `expectedOffset`，等于当前服务端位点才允许提交；
  不一致返回 **`INVALID_OFFSET`**（HTTP 400）。
- 位点为写入提供了“基于条件的提交”边界：调用方应先读取当前位点，再提交；
  并发竞争失败方拿到 `INVALID_OFFSET`，可带最新位点重试（幂等批次不会重复生效）。

### 2.2 幂等与重放

- 请求必须携带 `batchId`。服务端持久化每个批次的**回执**
  （状态、错误原因、内容指纹、快照标识、位点、时间）。
- 同一 `batchId` **内容相同**重复提交：直接返回首次结果，`replay=true`、
  `snapshotId` 与首次完全一致，**不产生新快照、不产生重复数据**。
- 同一 `batchId` **内容不同**：返回 **`DUPLICATE_BATCH_CONFLICT`**，绝不把新内容
  悄悄算到旧批次上。内容指纹为 `SHA-256(模式 + 规范化后的行内容)`。
- 状态为 `REJECTED` / `FAILED` 的批次允许用同一 `batchId` 修正后重试，新结果覆盖旧回执；
  只有 `COMMITTED` 严格只生效一次。
- 空内容、校验失败等**提交点之前**的失败：不前进位点，回执标记原因；
  唯一提交点是 state.json 的原子覆盖，因此不存在“提交了一半”的状态。

### 2.3 APPEND 与 UPSERT

| 模式 | 语义 | 快照文件 |
|---|---|---|
| `APPEND` | 仅追加；与已提交数据或批内出现相同主键即拒绝 `PK_CONFLICT` | 快照继承父快照文件列表 + 本批增量文件（delta 链） |
| `UPSERT` | 按主键合并：同键覆盖（保留原行序），新键追加；永不冲突 | 合并结果整体写入单个新文件 |

主键列缺失或为 null 返回 **`MISSING_PRIMARY_KEY`**。

### 2.4 提交协议与崩溃恢复

1. 校验位点、批次指纹去重、行类型、主键冲突（全部在持锁临界区内）；
2. 数据写入 `staging/`（此时对任何读不可见）；
3.（故障注入点，见 §6）；
4. 数据文件原子 move 到 `data/`；
5. **原子覆盖 state.json 发布新快照——唯一提交点**。

若进程在 3/4 阶段崩溃：链头快照不变、看不到半成品、位点不变；`staging/` 残留只在
**下一个持锁提交者进入临界区时**回收（不能在无锁读路径回收，否则会删掉其它提交者
正在发布的文件）。用同一 `batchId` 重放即安全完成。

### 2.5 可区分的拒绝 / 失败原因

| 错误码 | 含义 | HTTP |
|---|---|---|
| `BAD_REQUEST` | 请求结构非法（缺 batchId、列定义非法等） | 400 |
| `TABLE_NOT_FOUND` / `TABLE_ALREADY_EXISTS` | 表不存在 / 已存在 | 404 / 409 |
| `INVALID_OFFSET` | 位点与服务端不一致 | 400 |
| `PK_CONFLICT` | APPEND 主键与已有数据或批内重复 | 400 |
| `MISSING_PRIMARY_KEY` | 主键列缺失或为 null | 400 |
| `UNKNOWN_COLUMN` | 行包含当前模式之外的列 | 400 |
| `TYPE_MISMATCH` | 值类型与列声明不符（数字字符串允许转 LONG/DOUBLE） | 400 |
| `DUPLICATE_BATCH_CONFLICT` | 同 batchId 提交了不同内容 | 400 |
| `SCHEMA_INCOMPATIBLE` | 模式变更冲突 / 不兼容 | 400 |
| `BATCH_TIMEOUT` | 批次等待提交锁超时 | 408 |
| `MAINTENANCE_TIMEOUT` | 合并/清理等待提交锁超时 | 408 |
| `SNAPSHOT_NOT_FOUND` | 时间旅行目标快照不存在或已被清理 | 404 |

### 2.6 超时与并发边界

- 每张表一把提交锁：进程内用**公平 `ReentrantLock`**串行化，进程间用
  lock 文件上的 `FileLock` 互斥（支持多 JVM）。
- `tryLock(timeoutMs)` 提供明确超时边界：写入超时返回 `BATCH_TIMEOUT`，
  维护操作超时返回 `MAINTENANCE_TIMEOUT`，超时不会占锁、不会提交。
- 默认值可通过 `lakehouse.default-batch-timeout-ms` /
  `lakehouse.maintenance-timeout-ms` 配置，请求可用 `timeoutMs` 覆盖。

---

## 3. 模式兼容规则

列以**稳定数字 id** 标识，模式版本从 1 开始，每次变更 +1（一组变更原子应用）。

| 操作 | 是否允许 | 行为 |
|---|---|---|
| ADD COLUMN | 允许 | 新列获得新 id；可带 `defaultValue`（默认 null）。旧行读取时补默认值，并在响应 `defaultFilledColumns` 中明确标注 |
| RENAME COLUMN | 允许 | **只改列名，列 id 不变**；旧文件按 id 解码，绝不错列；若改名的是主键列，主键名同步更新 |
| DROP COLUMN | 允许 | 从当前模式移除；旧文件中的残留字段读取时忽略；**主键列禁止删除** |

**拒绝而非静默错列的场景**（`SCHEMA_INCOMPATIBLE` / `TYPE_MISMATCH`）：
新增重名列、重命名到已存在列名、对不存在的列改名/删除、删除主键列、
新列默认值与声明类型不符、变更缺少必要字段。本实现**不提供改类型**操作。

### 3.1 旧数据在新模式下的读取

- 旧行缺少新增列 id：按该列默认值填充（默认 `null`），响应中 `defaultFilledColumns`
  列出被填充的列名，默认值来源稳定可解释。
- 行内存在已删除列 id：忽略，不报错。
- 回读时按列声明类型规整（例如 JSON 把小整数解析成 `Integer`，服务端统一转回 `LONG`）。

### 3.2 快照隔离（新写入对旧快照不可见）

时间旅行读取 `?snapshot=<id>` 看到的是**该快照被提交那一刻的行集合**，之后的任何写入、
合并、模式变更都不会改变它的行数与行内容（只是按当前模式的列名/默认值解释）。
被 `expire` 清理掉的快照再读取返回 `SNAPSHOT_NOT_FOUND`。

---

## 4. 快照可见性与维护

- **不可变快照**：`snapshotId` 单调递增，保存父 id、模式版本、行数、数据文件列表、
  操作类型（APPEND/UPSERT/COMPACT）与 batchId。
- **读不加锁**：读取的是已提交快照引用的不可变文件，因此永远读不到半成品；
  并发写入期间读取到的永远是某个已提交快照（行数单调不减、主键唯一）。
- **小文件合并 COMPACT**：把当前链头快照全部数据重写为单个新文件并生成新快照；
  旧文件不立即删除。合并失败只影响本次操作，已有快照完好。
- **过期清理 EXPIRE**：保留链头起最近 N 个快照（默认 3，可用 `retain` 覆盖），
  移除更旧快照的元数据；数据文件仅当**不再被任何保留快照引用**时才删除
  （被后续 delta 链共享的文件不会误删）。清理失败不触碰已有快照。
- 合并、清理与写入使用**同一把提交锁**，彼此串行，从根本上避免“清理删掉正在读的文件、
  合并与写入互相覆盖”等问题；读路径不持锁所以不受维护阻塞。

---

## 5. HTTP API

服务启动后端口随机（`server.port=0`），本地可用固定端口运行：

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=8080"
```

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/tables` | 建表 `{table, columns:[{id,name,type}], primaryKey:[...]}` |
| GET | `/api/tables` | 列出表 |
| GET | `/api/tables/{t}` | 当前模式（版本、列、主键、链头快照） |
| POST | `/api/tables/{t}/writes` | 批次写入（见请求体示例） |
| GET | `/api/tables/{t}/rows[?snapshot=<id>]` | 当前读取 / 时间旅行 |
| POST | `/api/tables/{t}/schema` | 模式演进 `{changes:[{op,column,renameTo,dataType,defaultValue}]}` |
| POST | `/api/tables/{t}/compact[?timeoutMs=]` | 小文件合并 |
| POST | `/api/tables/{t}/expire[?retain=&timeoutMs=]` | 过期快照清理 |
| POST/DELETE | `/api/tables/{t}/fault` | 测试用故障注入开关（武装/清除） |

写入请求体：

```json
{
  "batchId": "batch-0001",
  "mode": "APPEND",
  "expectedOffset": 0,
  "timeoutMs": 10000,
  "rows": [ {"id": 1, "name": "alice", "amount": 12.5} ]
}
```

错误响应统一为 `{"error":"<错误码>","message":"<可解释细节>"}`。
所有关键操作都有结构化日志，统一 `[lakehouse]` / `[test]` 前缀，打印**位点、快照标识、
batchId、模式版本与判定依据（拒绝原因、默认填充列、删除文件等）**。

---

## 6. 验证方法

### 6.1 自动化测试（25 个用例，全部本地、无外部依赖）

```bash
mvn test
```

| 测试类 | 覆盖场景 |
|---|---|
| `WriteIdempotencyTest` | 重复提交回放、同 id 不同内容拒绝、UPSERT 覆盖且幂等 |
| `RejectionReasonTest` | 位点非法、主键冲突（跨表/批内）、类型不匹配、未知列、缺主键、被拒批次重试、**批次超时边界** |
| `CrashRecoveryTest` | 提交前注入崩溃：无半成品、位点不前进、staging 残留回收、重放只生效一次 |
| `SchemaEvolutionTest` | 增/删/改名读兼容、默认值填充、列不错列、新写入对旧快照不可见、不兼容变更拒绝 |
| `SnapshotMaintenanceTest` | 快照不可变、时间旅行、合并不丢数、清理只删孤儿文件、清理后旧快照 404、维护超时 |
| `ConcurrencyTest` | 200 并发 UPSERT 无丢失/重复/位点连续；1000+ 次并发读零违规；写入与 5 次合并 + 3 次清理并发最终一致 |
| `PersistenceRestartTest` | 重建引擎（模拟重启）后模式/回执/快照恢复，重放仍幂等 |
| `RestApiIntegrationTest` | HTTP 全流程 + 错误码到 HTTP 状态的映射 |

并发用例的做法：写线程在 `INVALID_OFFSET`/`BATCH_TIMEOUT` 时带退避重试，
断言最终行数、主键唯一性、快照数与最终位点严格一致。

### 6.2 手动 curl 冒烟

```bash
# 1) 建表
curl -s -X POST localhost:8080/api/tables -H 'Content-Type: application/json' -d '{
  "table":"orders","primaryKey":["id"],
  "columns":[{"id":1,"name":"id","type":"LONG"},
             {"id":2,"name":"name","type":"STRING"},
             {"id":3,"name":"amount","type":"DOUBLE"}]}'

# 2) 提交批次（expectedOffset=0）
curl -s -X POST localhost:8080/api/tables/orders/writes -H 'Content-Type: application/json' -d '{
  "batchId":"b1","mode":"APPEND","expectedOffset":0,
  "rows":[{"id":1,"name":"alice","amount":12.5}]}'

# 3) 重复提交（观察 replay=true、snapshotId 不变）
curl -s -X POST localhost:8080/api/tables/orders/writes -H 'Content-Type: application/json' -d '{
  "batchId":"b1","mode":"APPEND","expectedOffset":0,
  "rows":[{"id":1,"name":"alice","amount":12.5}]}'

# 4) 时间旅行 / 当前读
curl -s 'localhost:8080/api/tables/orders/rows?snapshot=1'
curl -s 'localhost:8080/api/tables/orders/rows'

# 5) 模式演进：新增 tag 列
curl -s -X POST localhost:8080/api/tables/orders/schema -H 'Content-Type: application/json' -d '{
  "changes":[{"op":"ADD","column":"tag","dataType":"STRING"}]}'

# 6) 合并与清理
curl -s -X POST localhost:8080/api/tables/orders/compact
curl -s -X POST 'localhost:8080/api/tables/orders/expire?retain=1'

# 7) 故障注入：下一次写入在“数据已写、状态未提交”处中断，再重放恢复
curl -s -X POST localhost:8080/api/tables/orders/fault
```

测试日志示例（位点、快照标识与判定依据）：

```
[lakehouse] batch-committed table=orders batchId=b1 mode=APPEND offset=0->1 rows=2 snapshotId=1 schemaVersion=1 file=snap-1-....jsonl
[lakehouse] batch-replay   table=orders batchId=b1 status=COMMITTED snapshotId=1 offset=1
[lakehouse] batch-rejected table=orders batchId=b3 reason=INVALID_OFFSET offset=1 detail=位点非法：expectedOffset=0，当前已提交位点=1
[lakehouse] read table=orders snapshotId=1 timeTravel=true rows=1 schemaVersion=2 defaultFilled=[tag]
[test] concurrent-upserts finalRows=200 distinctSnapshots=200 finalOffset=200 timeoutRejected=0
[test] concurrent-read-write reads=1171 violations=0 finalRows=100
```

### 6.3 配置项（`application.properties`）

```properties
lakehouse.data-dir=build/lakehouse-data        # 本地数据根目录
lakehouse.default-batch-timeout-ms=10000       # 批次锁等待默认超时
lakehouse.maintenance-timeout-ms=30000         # 合并/清理锁等待默认超时
lakehouse.default-retain-snapshots=3           # 清理时默认保留快照数
```

---

## 7. 关键设计取舍

- **单提交点 + 不可变文件**：一致性来自“数据文件先原子发布、state.json 原子覆盖提交”，
  而非复杂事务；崩溃恢复天然等价于批次重放。
- **列 id 而非列名作为物理标识**：重命名只是元数据变化，从根本上杜绝错列。
- **delta 文件链 + 全量合并**：APPEND 快照继承父文件列表（零重写），
  UPSERT/COMPACT 产生全量单文件；清理时按引用计数安全回收。
- **维护与写入共用表锁、读取无锁**：维护操作有强一致边界且不阻塞读，
  读路径仅访问不可变文件。
