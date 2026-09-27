# 本地湖仓表：增量写入与快照管理服务

一个**不依赖任何外部服务**的 Spring Boot 应用，在本地磁盘上提供湖仓表（Iceberg-lite）能力，用于验证：

1. 按批次写入与按主键 upsert 的幂等语义；
2. 新增 / 重命名 / 删除字段后的读取兼容与模式冲突拒绝；
3. 不可变快照、时间旅行读取，以及写入 / 读取 / 合并 / 清理并发时的一致性；
4. 崩溃 / 部分写入失败后的回滚与恢复。

数据全部落在本地目录（默认 `${java.io.tmpdir}/lakehouse-demo`，可用 `lakehouse.data-dir` 覆盖）。

---

## 1. 构建与运行

```bash
mvn spring-boot:run
# 指定本地目录
mvn spring-boot:run -Dspring-boot.run.arguments="--lakehouse.data-dir=/path/to/data"

mvn test            # 31 个测试：幂等/拒绝原因/模式演进/快照/崩溃恢复/并发
```

配置项（`application.properties`）：

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `lakehouse.data-dir` | `${java.io.tmpdir}/lakehouse-demo` | 本地数据根目录 |
| `lakehouse.commit-timeout` | `5s` | 获取串行提交锁的超时边界（写入 / 模式变更） |
| `lakehouse.maintenance-timeout` | `30s` | 合并 / 清理获取锁的超时边界 |

### 磁盘布局（每张表一个目录）

```
<data-dir>/<table>/
  metadata/
    head.json            # 唯一的可变指针：当前快照 id 与 seq（最后发布）
    commit-<seq>.json    # 不可变提交记录：快照清单 + 批次幂等台账（恢复的事实来源）
    schema-v<n>.json     # 不可变的带版本模式
  data/
    seq-<seq>-<uuid>.json  # 不可变数据文件（按稳定列 id 存值）
```

发布一次提交的顺序是：**先落不可变数据文件 → 再落不可变 commit 记录 → 最后原子改 head 指针 → 更新内存态**。
所有文件写入都走“写临时文件 + 原子 rename”，因此任何时刻崩溃都不会产生半成品文件。

---

## 2. REST API

### 建表
`POST /api/tables/{name}`
```json
{
  "columns": [
    {"id": 1, "name": "id",    "type": "LONG",   "nullable": false},
    {"id": 2, "name": "name",  "type": "STRING", "nullable": true},
    {"id": 3, "name": "score", "type": "DOUBLE", "nullable": true}
  ],
  "primaryKey": "id"
}
```
- `201` 成功；表已存在返回 `409 TABLE_EXISTS`；主键列必须存在且非空。

### 批次写入 / upsert
`POST /api/tables/{name}/batches`
```json
{
  "batchId": "b-1",
  "expectedOffset": 0,
  "mode": "APPEND",
  "rows": [ {"valuesByColumnId": {"1": 1, "2": "alice", "3": 1.5}} ]
}
```
`mode` 为 `APPEND` 或 `UPSERT`。**行按列 id（不是列名/位置）存值**。

成功：`{"accepted":true,"replay":false,"snapshotId":"s2","newOffset":1,...}`
业务拒绝：HTTP 200，响应体里 `accepted=false` 并带 `rejectReason`（便于程序分支处理）。

### 模式演进
`POST /api/tables/{name}/schema`
```json
{"op":"ADD","name":"remark","type":"STRING","nullable":true}
{"op":"RENAME","name":"name","to":"full_name"}
{"op":"DROP","name":"score"}
```

### 读取
- `GET /api/tables/{name}/rows`：读当前 head，按**最新模式**投影；
- `GET /api/tables/{name}/snapshots/{snapshotId}/rows`：时间旅行，按**该快照自己的模式**读取；
  - 快照不存在或已过期：`410 UNKNOWN_SNAPSHOT`；表不存在：`404 UNKNOWN_TABLE`。

每行除 `values` 外还返回 `sources`，逐字段解释取值：
`FROM_DATA`（来自数据文件）或 `DEFAULT_NULL`（该行写入之后才新增的列，稳定默认 null）。

### 维护
- `POST /api/tables/{name}/compact`：把 head 视图的多个小文件按主键合并成一个新文件，产生新快照；
- `POST /api/tables/{name}/expire?retain=N`：保留最新 N 个快照，过期其余快照并回收其独占文件。

---

## 3. 写入语义与拒绝原因

### 幂等
- 每个批次由调用方提供唯一 `batchId`，提交成功后记入持久化台账。
- **相同 `batchId` + 相同负载/位点/模式**重复提交：不再写任何数据，返回 `accepted=true, replay=true`，
  且 `snapshotId` 与首次完全一致。并发重放同一批次也只会有一次真正提交（见 `ConcurrencyTest`）。
- 相同 `batchId` 但**负载/位点/模式不同**：拒绝 `BATCH_ID_REUSE`，防止把重放伪装成新数据。
- 批次负载一致性由提交清单中的签名字段判定。

### 位点（offset）
- 表维护单调位点 `nextOffset`，初始为 0，每个成功批次 `+1`。
- `expectedOffset` 必须严格等于当前位点，否则拒绝 `INVALID_OFFSET`（区分重复、乱序、跳号）。
- 位点不合法的批次在落任何数据文件之前即被拒绝，位点不前进。

### 主键
- `APPEND`：主键已存在 → `PK_CONFLICT`（不覆盖）；同一批次内主键重复同样整体拒绝。
- `UPSERT`：按主键更新，读取时跨数据文件按“文件 seq 更大者胜”合并，同一主键只保留最新版本。

### 类型
- 整批先做模式/类型校验，再落盘：类型不符（如 STRING 列给数字）、缺少非空列（含主键）、
  引用未知列 id → `TYPE_MISMATCH`，整批拒绝，不留任何文件、位点不变。
- 主键比较做了数值规范化：JSON 中小整数可能反序列化为 `Integer`、大整数为 `Long`，
  合并与冲突判断按逻辑值（LONG/DOUBLE 规范化）进行，不会误判为不同键。

### 超时
- 写入 / 模式变更 / 合并 / 清理共用一把公平的串行写锁，`tryLock` 带超时。
- 写入等锁超时 → `BATCH_TIMEOUT`；维护等锁超时 → `MAINTENANCE_BUSY`，两者明确区分。

### 部分写入失败 / 崩溃
- 校验通过后，写数据文件或提交记录失败：返回 `IO_FAILURE`，**head 未动 → 对外完全无影响**；
  暂存的孤儿文件在下次打开表时由恢复流程确定性回收。
- 若提交记录已落但 head 指针尚未推进就崩溃：恢复时该提交被视为**中止事务**，
  其记录与数据文件被清除，绝不会出现“数据在但 head 不认”或半批可见。

---

## 4. 模式兼容规则

列由**稳定的数字 id** 标识，重命名不改变 id，因此永远不会因位置/名称变化而“静默错列”。

| 变更 | 规则 | 对旧数据的读取 |
| --- | --- | --- |
| ADD 可空列 | 名称不冲突即可 | 旧行该列稳定为 `null`，`sources` 标 `DEFAULT_NULL` |
| ADD 非空列 | 表非空时**拒绝** `SCHEMA_INCOMPATIBLE`（无回填手段，避免产生非法旧行）；空表允许 | — |
| RENAME | 源列必须存在、目标名不得与现有列重名，否则 `SCHEMA_INCOMPATIBLE` | 列 id 不变，值原样保留，新视图用新名 |
| DROP | 列必须存在；**禁止删除主键列** | 新视图无该列；时间旅行旧快照中数据仍在 |

- 每次模式变更产生一个 `SCHEMA` 快照（不可变 `schema-v<n>.json`）。
- **读取解释规则**：`readAt(snapshotId)` 永远用该快照自己的模式；`readHead()` 用最新模式投影。
  因此旧快照天然看不到之后新增的列与之后写入的行（快照的数据文件清单本身不可变）。
- 冲突 / 不兼容（重名、重命名到已存在名字、删主键、给非空表加非空列）一律拒绝，绝不静默错列。

---

## 5. 快照可见性与并发一致性

- **不可变快照**：快照 = `{schemaVersion, 不可变数据文件清单, nextOffset, kind, parentSeq}`，一经写出不改。
- **时间旅行**：任何历史快照（未过期）都能按当时模式完整读取；合并 / 清理只新增快照，不改旧快照。
- **小文件合并**：读取 head 当前视图（按主键 upsert 合并）→ 写一个新不可变文件 → 发布指向它的新快照。
  旧文件仍被旧快照引用，旧时间旅行读取不受影响；合并失败只清理新暂存文件，head 与所有旧快照不变。
- **快照清理**：先发布新的 `EXPIRE` 边界快照（让旧快照不可达），**然后**尽力删除过期快照独占的
  数据文件与提交记录。物理删除失败不影响正确性（文件已不可达），下次打开时按“不被任何存活快照引用”
  规则回收；保留快照引用的文件绝不删除，head 永远保留。
- **并发**：
  - 写 / 模式 / compact / expire 由一把公平写锁串行化，提交对调用方表现为原子；
  - 读取读锁，只读不可变快照与不可变文件，与合并、写入并发时不会读到半成品、不会丢数据或重复；
  - 等锁有超时边界（`BATCH_TIMEOUT` / `MAINTENANCE_BUSY`），不会无限等待。

### 恢复算法（打开表时）
1. 读 `head.json` 拿到权威 `seq`；按 seq 排序回放所有 `commit-<seq>.json`；
2. `seq > head.seq` 的提交记录视为中止事务，删除其数据文件与记录；
3. 依次应用 EXPIRE 快照携带的“已过期 seq 列表”，移除过期快照与台账外状态；
4. 以 head 快照的 offset 与最高 schema 版本为准重建内存；
5. 删除数据目录中**不被任何存活快照引用**的文件（崩溃留下的暂存孤儿）。

---

## 6. 如何验证

`mvn test` 共 31 个测试（见 `src/test/.../tests/lakehouse`），关键判定均通过
SLF4J 日志与 `[VERDICT:...]` 标准输出打印**位点、快照标识和判定依据**（surefire 报告在
`target/surefire-reports/`）：

| 测试类 | 覆盖点 |
| --- | --- |
| `WriteSemanticsTest` | 重放幂等、同 id 不同负载拒绝、非法位点、APPEND 主键冲突（批间/批内）、类型不符、缺主键、UPSERT 更新 |
| `SchemaEvolutionTest` | 加列默认值及 `DEFAULT_NULL` 来源、非空列拒绝、重命名不错列、重命名冲突、删列/删主键、旧快照对新写入不可见 |
| `SnapshotMaintenanceTest` | 时间旅行、未知快照区分、合并不丢行且旧快照仍可读、upsert+合并去重、过期快照不可读及独占文件回收、重复清理稳定 |
| `FailureRecoveryTest` | 暂存阶段失败零写入、提交记录后/head 前崩溃被当中止事务回收、重启重建位点与幂等台账、校验失败不落文件 |
| `ConcurrencyTest` | 20 线程并发 APPEND 不丢不重、同批次并发重放恰好一次提交、合并期间持续读到完整一致视图、超时边界 `BATCH_TIMEOUT` vs `MAINTENANCE_BUSY` |
| `ApiIntegrationTest` | HTTP 全生命周期：建表/写入/重放/各类拒绝/模式/时间旅行，以及 `409/410/404` 状态码 |

手工冒烟（上文脚本等价）：

```bash
curl -s -X POST localhost:$PORT/api/tables/demo -H 'Content-Type: application/json' -d '{"columns":[{"id":1,"name":"id","type":"LONG","nullable":false}],"primaryKey":"id"}'
curl -s -X POST localhost:$PORT/api/tables/demo/batches -H 'Content-Type: application/json' \
  -d '{"batchId":"b1","expectedOffset":0,"mode":"APPEND","rows":[{"valuesByColumnId":{"1":1}}]}'
curl -s localhost:$PORT/api/tables/demo/rows
```
