package com.github.lisama569.lakehousesnapshotservicejava.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.lakehousesnapshotservicejava.exception.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.model.BatchReceipt;
import com.github.lisama569.lakehousesnapshotservicejava.model.BatchStatus;
import com.github.lisama569.lakehousesnapshotservicejava.model.CleanupResult;
import com.github.lisama569.lakehousesnapshotservicejava.model.ColumnDef;
import com.github.lisama569.lakehousesnapshotservicejava.model.ReadResult;
import com.github.lisama569.lakehousesnapshotservicejava.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.model.RowRecord;
import com.github.lisama569.lakehousesnapshotservicejava.model.SchemaChange;
import com.github.lisama569.lakehousesnapshotservicejava.model.SchemaResult;
import com.github.lisama569.lakehousesnapshotservicejava.model.SnapshotInfo;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteMode;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteRequest;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteResult;
import com.github.lisama569.lakehousesnapshotservicejava.storage.AtomicFiles;
import com.github.lisama569.lakehousesnapshotservicejava.storage.LockManager;
import com.github.lisama569.lakehousesnapshotservicejava.storage.TableState;
import com.github.lisama569.lakehousesnapshotservicejava.storage.TableStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * 单张本地湖仓表。
 *
 * <p>一致性约定：</p>
 * <ul>
 *   <li>所有状态变更（写入 / 演进 / 合并 / 清理）都在表级提交锁内完成，串行执行；</li>
 *   <li>数据文件不可变：先写 staging，再原子发布到 data/，最后原子覆盖 state.json 提交快照；</li>
 *   <li>提交点只有一个（state.json 的原子覆盖），崩溃后重放同一批次会命中回执，结果幂等；</li>
 *   <li>读路径不加锁：读取的是已提交快照引用的不可变文件，绝不读到半成品。</li>
 * </ul>
 */
public class LocalTable {

    private static final Logger log = LoggerFactory.getLogger(LocalTable.class);

    private final String tableName;
    private final TableStateStore store;
    private final LockManager lockManager;
    private final ObjectMapper mapper;
    private final FaultControl faultControl;
    private final long defaultLockTimeoutMs;

    public LocalTable(String tableName,
                      TableStateStore store,
                      LockManager lockManager,
                      ObjectMapper mapper,
                      FaultControl faultControl,
                      long defaultLockTimeoutMs) {
        this.tableName = tableName;
        this.store = store;
        this.lockManager = lockManager;
        this.mapper = mapper;
        this.faultControl = faultControl;
        this.defaultLockTimeoutMs = defaultLockTimeoutMs;
    }

    // ------------------------------------------------------------------
    // 写入
    // ------------------------------------------------------------------

    public WriteResult write(WriteRequest request) {
        if (request == null) {
            throw new LakehouseException(RejectReason.BAD_REQUEST, "写入请求为空");
        }
        String batchId = request.batchId();
        if (batchId == null || batchId.isBlank()) {
            throw new LakehouseException(RejectReason.BAD_REQUEST, "batchId 不能为空");
        }
        WriteMode mode = request.mode() == null ? WriteMode.APPEND : request.mode();
        List<RowRecord> rows = request.rows() == null ? List.of() : request.rows();
        long timeoutMs = request.timeoutMs() > 0 ? request.timeoutMs() : defaultLockTimeoutMs;
        String payloadHash = payloadHash(mode, rows);

        try (AutoCloseable ignored = lockManager.acquire(
                tableName, store.lockFile(tableName), timeoutMs, RejectReason.BATCH_TIMEOUT)) {
            TableState state = requireState();
            // 临界区独占：此处回收的必是上次崩溃残留，绝不会碰到其它提交者正在发布的文件。
            AtomicFiles.reapStaging(store.stagingDir(tableName));
            long offsetBefore = state.committedOffset;

            // 1) 幂等去重：同一 batchId 已处理过，直接回放首次结果。
            WriteResult replay = checkReplay(state, batchId, payloadHash);
            if (replay != null) {
                return replay;
            }

            try {
                // 2) 位点校验：expectedOffset 必须等于当前已提交位点。
                validateOffset(state, request.expectedOffset(), batchId);

                // 3) 行校验与主键冲突检测（APPEND 时）。
                List<Map<String, Object>> normalized = new ArrayList<>(rows.size());
                for (RowRecord r : rows) {
                    normalized.add(RowCodec.validateAndNormalize(state, r));
                }
                if (mode == WriteMode.APPEND) {
                    detectPkConflicts(state, normalized, batchId);
                }

                // 4) 计算本次数据文件最终内容：
                //    APPEND 为增量行（快照继承父文件列表）；UPSERT 为全量合并结果（单文件）。
                SnapshotInfo parent = state.snapshotById(state.headSnapshotId);
                List<String> dataFiles;
                long recordCount;
                List<Map<String, Object>> fileRows;
                if (mode == WriteMode.UPSERT) {
                    fileRows = mergeUpsert(state, normalized);
                    dataFiles = null; // 占位，发布后赋值
                    recordCount = fileRows.size();
                } else {
                    fileRows = normalized;
                    dataFiles = new ArrayList<>();
                    if (parent != null) {
                        dataFiles.addAll(parent.dataFiles());
                    }
                    recordCount = (parent == null ? 0 : parent.recordCount()) + rows.size();
                }

                // 5) 写 staging 数据文件（尚未发布，崩溃不会留下半成品快照）。
                Path staging = writeStaging(state, fileRows);

                // 6) 故障注入点：数据文件已就绪、状态未提交。模拟进程崩溃。
                if (faultControl.consumeCrashBeforeCommit()) {
                    log.warn("[lakehouse] fault-injected-crash table={} batchId={} stage=pre-commit",
                            tableName, batchId);
                    throw new IllegalStateException("模拟崩溃：提交前中断 batchId=" + batchId);
                }

                // 7) 原子发布数据文件并提交快照（state.json 原子覆盖是唯一提交点）。
                long snapshotId = state.nextSnapshotId++;
                String fileName = "snap-" + snapshotId + "-" + UUID.randomUUID() + ".jsonl";
                Path dataFile = store.dataDir(tableName).resolve(fileName);
                AtomicFiles.publish(staging, dataFile);

                if (mode == WriteMode.UPSERT) {
                    dataFiles = List.of(fileName);
                } else {
                    dataFiles.add(fileName);
                }

                SnapshotInfo snapshot = new SnapshotInfo(
                        snapshotId, state.headSnapshotId, state.schemaVersion,
                        System.currentTimeMillis(), recordCount,
                        dataFiles, mode.name(), batchId);
                state.snapshots.add(snapshot);
                state.headSnapshotId = snapshotId;
                state.committedOffset = offsetBefore + 1;
                state.batches.put(batchId, new BatchReceipt(batchId, BatchStatus.COMMITTED, null,
                        payloadHash, snapshotId, state.committedOffset, System.currentTimeMillis(), null));
                store.persist(state);

                log.info("[lakehouse] batch-committed table={} batchId={} mode={} offset={}->{} "
                                + "rows={} snapshotId={} schemaVersion={} file={}",
                        tableName, batchId, mode, offsetBefore, state.committedOffset,
                        rows.size(), snapshotId, state.schemaVersion, fileName);
                return new WriteResult(tableName, batchId, BatchStatus.COMMITTED, null,
                        snapshotId, state.schemaVersion, state.committedOffset,
                        rows.size(), false, null);
            } catch (LakehouseException e) {
                recordReceipt(state, batchId, payloadHash, BatchStatus.REJECTED, e.getReason(),
                        e.getMessage());
                log.warn("[lakehouse] batch-rejected table={} batchId={} reason={} offset={} detail={}",
                        tableName, batchId, e.getReason(), state.committedOffset, e.getMessage());
                throw e;
            } catch (RuntimeException e) {
                recordReceipt(state, batchId, payloadHash, BatchStatus.FAILED, null, e.getMessage());
                log.error("[lakehouse] batch-failed table={} batchId={} offset={} detail={}",
                        tableName, batchId, state.committedOffset, e.getMessage(), e);
                throw e;
            }
        } catch (LakehouseException | IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("提交锁处理异常 table=" + tableName, e);
        }
    }

    /** 已处理批次的回放逻辑：仅 COMMITTED 严格回放；REJECTED/FAILED 均允许修正后重试。 */
    private WriteResult checkReplay(TableState state, String batchId, String payloadHash) {
        BatchReceipt receipt = state.batches.get(batchId);
        if (receipt == null || receipt.status() != BatchStatus.COMMITTED) {
            if (receipt != null) {
                log.info("[lakehouse] batch-retry table={} batchId={} prevStatus={} prevReason={} "
                                + "offset={}",
                        tableName, batchId, receipt.status(), receipt.rejectReason(),
                        state.committedOffset);
            }
            return null; // REJECTED / FAILED 允许重试，新结果覆盖旧回执
        }
        if (!receipt.payloadHash().equals(payloadHash)) {
            throw new LakehouseException(RejectReason.DUPLICATE_BATCH_CONFLICT,
                    "batchId=" + batchId + " 已提交过不同内容，拒绝重放");
        }
        log.info("[lakehouse] batch-replay table={} batchId={} status={} snapshotId={} offset={}",
                tableName, batchId, receipt.status(), receipt.snapshotId(), receipt.offset());
        return new WriteResult(tableName, batchId, receipt.status(), receipt.rejectReason(),
                receipt.snapshotId(), state.schemaVersion, receipt.offset(), 0, true,
                receipt.detail());
    }

    private void validateOffset(TableState state, long expectedOffset, String batchId) {
        if (expectedOffset != state.committedOffset) {
            throw new LakehouseException(RejectReason.INVALID_OFFSET,
                    "位点非法：expectedOffset=" + expectedOffset
                            + "，当前已提交位点=" + state.committedOffset + "（batchId=" + batchId + "）");
        }
    }

    private void detectPkConflicts(TableState state, List<Map<String, Object>> rows, String batchId) {
        Set<String> seen = new HashSet<>();
        for (Map<String, Object> row : rows) {
            String key = pkKey(state, row);
            if (!seen.add(key)) {
                throw new LakehouseException(RejectReason.PK_CONFLICT,
                        "批次内主键重复 key=" + key + "（batchId=" + batchId + "）");
            }
        }
        if (state.headSnapshotId > 0) {
            Set<String> existing = new HashSet<>();
            for (Map<String, Object> row : readSnapshotRows(state, state.headSnapshotId)) {
                existing.add(pkKey(state, row));
            }
            for (Map<String, Object> row : rows) {
                String key = pkKey(state, row);
                if (existing.contains(key)) {
                    throw new LakehouseException(RejectReason.PK_CONFLICT,
                            "主键与已提交数据冲突 key=" + key + "（batchId=" + batchId + "）");
                }
            }
        }
    }

    private String pkKey(TableState state, Map<String, Object> row) {
        StringBuilder sb = new StringBuilder();
        for (String pk : state.primaryKey) {
            sb.append(pk).append('=').append(row.get(pk)).append(';');
        }
        return sb.toString();
    }

    /** UPSERT：以 head 快照为基础，按主键合并（同键覆盖、保留原顺序，新键追加到末尾）。 */
    private List<Map<String, Object>> mergeUpsert(TableState state, List<Map<String, Object>> incoming) {
        Map<String, Map<String, Object>> merged = new LinkedHashMap<>();
        for (Map<String, Object> row : readSnapshotRows(state, state.headSnapshotId)) {
            merged.put(pkKey(state, row), row);
        }
        for (Map<String, Object> row : incoming) {
            merged.put(pkKey(state, row), row);
        }
        return new ArrayList<>(merged.values());
    }

    // ------------------------------------------------------------------
    // 读取（不加锁，读取已提交快照的不可变文件）
    // ------------------------------------------------------------------

    /**
     * 读取表数据。
     *
     * @param snapshotId null 表示读当前链头；否则时间旅行到指定快照（必须仍存在）。
     */
    public ReadResult read(Long snapshotId) {
        TableState state = requireState();
        boolean timeTravel = snapshotId != null;
        long target = timeTravel ? snapshotId : state.headSnapshotId;
        if (target == 0) {
            return new ReadResult(tableName, 0L, state.schemaVersion, state.committedOffset,
                    false, List.of(), List.of());
        }
        SnapshotInfo snap = state.snapshotById(target);
        if (snap == null) {
            throw new LakehouseException(RejectReason.SNAPSHOT_NOT_FOUND,
                    "快照不存在或已过期清理 table=" + tableName + " snapshotId=" + target);
        }
        List<RowRecord> rows = new ArrayList<>();
        Set<String> filled = new LinkedHashSet<>();
        for (String file : snap.dataFiles()) {
            for (Map<String, Object> encoded : readDataFile(file)) {
                RowCodec.DecodedRow decoded = RowCodec.decode(state, encoded);
                rows.add(new RowRecord(decoded.values()));
                filled.addAll(decoded.defaultFilledColumns());
            }
        }
        log.info("[lakehouse] read table={} snapshotId={} timeTravel={} rows={} "
                        + "schemaVersion={} defaultFilled={}",
                tableName, target, timeTravel, rows.size(), state.schemaVersion, filled);
        return new ReadResult(tableName, target, state.schemaVersion, state.committedOffset,
                timeTravel, List.copyOf(filled), rows);
    }

    public SchemaResult schema() {
        TableState state = requireState();
        return new SchemaResult(tableName, state.schemaVersion,
                List.copyOf(state.columns), List.copyOf(state.primaryKey),
                state.headSnapshotId, System.currentTimeMillis());
    }

    // ------------------------------------------------------------------
    // 模式演进
    // ------------------------------------------------------------------

    public void evolve(SchemaChange change) {
        try (AutoCloseable ignored = lockManager.acquire(
                tableName, store.lockFile(tableName), defaultLockTimeoutMs,
                RejectReason.MAINTENANCE_TIMEOUT)) {
            TableState state = requireState();
            int before = state.schemaVersion;
            SchemaEvolution.apply(state, change);
            store.persist(state);
            log.info("[lakehouse] schema-evolved table={} version={}->{} op={} column={}",
                    tableName, before, state.schemaVersion, change.op(), change.column());
        } catch (LakehouseException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("模式演进异常 table=" + tableName, e);
        }
    }

    // ------------------------------------------------------------------
    // 维护：小文件合并 / 快照清理
    // ------------------------------------------------------------------

    /**
     * 小文件合并：把当前链头快照的全部数据重写为单个数据文件，生成新快照。
     * 旧文件不被删除（由 expireSnapshots 统一回收），合并失败不影响已有快照。
     */
    public WriteResult compact(long timeoutMs) {
        long timeout = timeoutMs > 0 ? timeoutMs : defaultLockTimeoutMs;
        try (AutoCloseable ignored = lockManager.acquire(
                tableName, store.lockFile(tableName), timeout, RejectReason.MAINTENANCE_TIMEOUT)) {
            TableState state = requireState();
            AtomicFiles.reapStaging(store.stagingDir(tableName));
            if (state.headSnapshotId == 0) {
                log.info("[lakehouse] compact-skip table={} reason=empty-table", tableName);
                return new WriteResult(tableName, null, BatchStatus.COMMITTED, null,
                        0L, state.schemaVersion, state.committedOffset, 0, false,
                        "空表无需合并");
            }
            List<Map<String, Object>> rows = readSnapshotRows(state, state.headSnapshotId);

            long snapshotId = state.nextSnapshotId++;
            String fileName = "snap-" + snapshotId + "-" + UUID.randomUUID() + ".jsonl";
            Path staging = store.stagingDir(tableName).resolve(fileName + ".tmp");
            writeRowsToFile(state, staging, rows);
            AtomicFiles.publish(staging, store.dataDir(tableName).resolve(fileName));

            SnapshotInfo snapshot = new SnapshotInfo(snapshotId, state.headSnapshotId,
                    state.schemaVersion, System.currentTimeMillis(), rows.size(),
                    List.of(fileName), "COMPACT", null);
            state.snapshots.add(snapshot);
            state.headSnapshotId = snapshotId;
            store.persist(state);

            log.info("[lakehouse] compacted table={} snapshotId={} rows={} file={} schemaVersion={}",
                    tableName, snapshotId, rows.size(), fileName, state.schemaVersion);
            return new WriteResult(tableName, null, BatchStatus.COMMITTED, null,
                    snapshotId, state.schemaVersion, state.committedOffset, rows.size(),
                    false, "COMPACT");
        } catch (LakehouseException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("合并异常 table=" + tableName, e);
        }
    }

    /**
     * 过期快照清理：保留链头起最近 retainCount 个快照，其余快照的元数据移除、
     * 不再被任何保留快照引用的数据文件删除。清理失败只影响本次操作，不破坏已有快照。
     */
    public CleanupResult expireSnapshots(int retainCount, long timeoutMs) {
        if (retainCount < 1) {
            throw new LakehouseException(RejectReason.BAD_REQUEST, "retainCount 必须 >= 1");
        }
        long timeout = timeoutMs > 0 ? timeoutMs : defaultLockTimeoutMs;
        try (AutoCloseable ignored = lockManager.acquire(
                tableName, store.lockFile(tableName), timeout, RejectReason.MAINTENANCE_TIMEOUT)) {
            TableState state = requireState();
            AtomicFiles.reapStaging(store.stagingDir(tableName));
            List<Long> ids = state.snapshots.stream().map(SnapshotInfo::snapshotId).toList();
            List<Long> removed = new ArrayList<>();
            if (ids.size() > retainCount) {
                int removeUpTo = ids.size() - retainCount;
                List<SnapshotInfo> kept = new ArrayList<>();
                for (int i = 0; i < state.snapshots.size(); i++) {
                    if (i < removeUpTo) {
                        removed.add(state.snapshots.get(i).snapshotId());
                    } else {
                        kept.add(state.snapshots.get(i));
                    }
                }
                state.snapshots = kept;
            }
            // 计算仍被引用的数据文件，删除孤儿文件。
            Set<String> referenced = new HashSet<>();
            for (SnapshotInfo s : state.snapshots) {
                referenced.addAll(s.dataFiles());
            }
            List<String> deleted = new ArrayList<>();
            Path dataDir = store.dataDir(tableName);
            if (Files.isDirectory(dataDir)) {
                try (var stream = Files.list(dataDir)) {
                    for (Path f : stream.filter(Files::isRegularFile).toList()) {
                        if (!referenced.contains(f.getFileName().toString())) {
                            AtomicFiles.deleteIfExistsQuietly(f);
                            deleted.add(f.getFileName().toString());
                        }
                    }
                }
            }
            store.persist(state);
            log.info("[lakehouse] snapshots-expired table={} retained={} removedSnapshots={} "
                            + "deletedFiles={} headSnapshotId={}",
                    tableName, state.snapshots.size(), removed, deleted, state.headSnapshotId);
            return new CleanupResult(tableName, state.snapshots.size(), removed, deleted,
                    state.headSnapshotId);
        } catch (LakehouseException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("快照清理异常 table=" + tableName, e);
        }
    }

    // ------------------------------------------------------------------
    // 内部工具
    // ------------------------------------------------------------------

    private TableState requireState() {
        TableState state = store.load(tableName);
        if (state == null) {
            throw new LakehouseException(RejectReason.TABLE_NOT_FOUND, "表不存在: " + tableName);
        }
        return state;
    }

    private void recordReceipt(TableState state, String batchId, String payloadHash,
                               BatchStatus status, RejectReason reason, String detail) {
        state.batches.put(batchId, new BatchReceipt(batchId, status, reason, payloadHash,
                state.headSnapshotId, state.committedOffset, System.currentTimeMillis(), detail));
        store.persist(state);
    }

    private Path writeStaging(TableState state, List<Map<String, Object>> rows) {
        String name = "batch-" + UUID.randomUUID() + ".jsonl.tmp";
        Path staging = store.stagingDir(tableName).resolve(name);
        writeRowsToFile(state, staging, rows);
        return staging;
    }

    private void writeRowsToFile(TableState state, Path file, List<Map<String, Object>> rows) {
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
            StringBuilder sb = new StringBuilder();
            for (Map<String, Object> row : rows) {
                sb.append(mapper.writeValueAsString(RowCodec.encode(state, row))).append('\n');
            }
            Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("写数据文件失败: " + file, e);
        }
    }

    /** 读取某快照的逻辑行（按当前模式解码）。快照不存在返回空列表（空表场景）。 */
    private List<Map<String, Object>> readSnapshotRows(TableState state, long snapshotId) {
        if (snapshotId == 0) {
            return List.of();
        }
        SnapshotInfo snap = state.snapshotById(snapshotId);
        if (snap == null) {
            return List.of();
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String file : snap.dataFiles()) {
            for (Map<String, Object> encoded : readDataFile(file)) {
                rows.add(RowCodec.decode(state, encoded).values());
            }
        }
        return rows;
    }

    private List<Map<String, Object>> readDataFile(String fileName) {
        Path file = store.dataDir(tableName).resolve(fileName);
        if (!Files.isRegularFile(file)) {
            throw new LakehouseException(RejectReason.SNAPSHOT_NOT_FOUND,
                    "数据文件缺失 table=" + tableName + " file=" + fileName);
        }
        try {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> row = mapper.readValue(line, Map.class);
                    rows.add(row);
                }
            }
            return rows;
        } catch (IOException e) {
            throw new UncheckedIOException("读数据文件失败: " + file, e);
        }
    }

    /** 批次内容指纹：模式 + 行内容（规范化 JSON）的 SHA-256，用于识别“同 id 不同内容”。 */
    private String payloadHash(WriteMode mode, List<RowRecord> rows) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(mode.name().getBytes(StandardCharsets.UTF_8));
            for (RowRecord r : rows) {
                md.update(mapper.writeValueAsString(r.values()).getBytes(StandardCharsets.UTF_8));
                md.update((byte) '\n');
            }
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new IllegalStateException("计算批次指纹失败", e);
        }
    }
}
