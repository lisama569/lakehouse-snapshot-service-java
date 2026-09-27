package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.BatchOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.BatchRequest;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ColumnDef;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.CommittedBatch;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.MaintenanceOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ReadOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.RowRecord;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.RowView;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.SchemaChange;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.SchemaChangeOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.SnapshotManifest;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.TableSchema;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.WriteMode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single local lakehouse table.
 *
 * <p>Concurrency model:
 * <ul>
 *   <li>writes/schema-evolution/compaction/expiry are serialized by one write lock
 *       (acquired with a timeout -> BATCH_TIMEOUT / MAINTENANCE_BUSY);</li>
 *   <li>reads take the read lock and operate on immutable snapshots + immutable data
 *       files, so a concurrent compaction or expiry that keeps a snapshot never disturbs them;</li>
 *   <li>publication is crash-safe: stage data file -> durable immutable commit record
 *       -> publish single mutable head pointer -> apply in-memory state.</li>
 * </ul>
 */
public class LakehouseTable {

    private static final Logger log = LoggerFactory.getLogger(LakehouseTable.class);
    private static final JsonMapper MAPPER = new JsonMapper();

    private final LocalFileStore store;
    private final TablePaths paths;
    private final String name;
    private final TableState state = new TableState();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private LakehouseTable(LocalFileStore store, TablePaths paths, String name) {
        this.store = store;
        this.paths = paths;
        this.name = name;
        this.store.ensureDirectory(paths.metadataDir());
        this.store.ensureDirectory(paths.dataDir());
    }

    public static LakehouseTable create(LocalFileStore store, TablePaths paths, String name,
                                        List<ColumnDef> columns, String primaryKeyName) {
        LakehouseTable table = new LakehouseTable(store, paths, name);
        table.initialize(columns, primaryKeyName);
        return table;
    }

    public static LakehouseTable recover(LocalFileStore store, TablePaths paths, String name) {
        LakehouseTable table = new LakehouseTable(store, paths, name);
        table.recoverFromJournal();
        return table;
    }

    public String name() {
        return name;
    }

    public String headSnapshotId() {
        return state.headSnapshotId();
    }

    public LocalFileStore store() {
        return store;
    }

    public TablePaths paths() {
        return paths;
    }

    public TableSchema schema() {
        return state.schema();
    }

    public long nextOffset() {
        return state.nextOffset();
    }

    public List<SnapshotManifest> snapshots() {
        lock.readLock().lock();
        try {
            return state.snapshotsInOrder();
        } finally {
            lock.readLock().unlock();
        }
    }

    // ---------------------------------------------------------------- create --

    private void initialize(List<ColumnDef> requested, String primaryKeyName) {
        if (requested == null || requested.isEmpty()) {
            throw new IllegalArgumentException("at least one column is required");
        }
        List<String> names = new ArrayList<>();
        List<ColumnDef> columns = new ArrayList<>();
        int id = 0;
        for (ColumnDef c : requested) {
            id++;
            if (c == null || c.name() == null || c.name().isBlank() || c.type() == null) {
                throw new IllegalArgumentException("invalid column definition: " + c);
            }
            if (names.contains(c.name())) {
                throw new IllegalArgumentException("duplicate column name: " + c.name());
            }
            names.add(c.name());
            columns.add(new ColumnDef(id, c.name(), c.type(), c.nullable()));
        }
        int pkIndex = names.indexOf(primaryKeyName);
        if (pkIndex < 0) {
            throw new IllegalArgumentException("primary key column not found: " + primaryKeyName);
        }
        ColumnDef pk = columns.get(pkIndex);
        if (pk.nullable()) {
            throw new IllegalArgumentException("primary key column must be non-nullable: " + pk.name());
        }
        TableSchema schema = new TableSchema(1, id + 1, pk.id(), List.copyOf(columns));
        store.writeJsonAtomically(paths.schemaFile(1), schema);
        SnapshotManifest init = new SnapshotManifest(snapshotIdFor(1), 0L, 1L,
                System.currentTimeMillis(), 1, 0L, List.of(), "INIT", "table initialized", List.of());
        publish(new CommitRecord(1L, init, null), schema, List.of());
        log.info("table[{}] created with schema v1 pk={} snapshot={}",
                name, pk.name(), init.snapshotId());
    }

    // ------------------------------------------------------------- recovery --

    private void recoverFromJournal() {
        HeadPointer head = store.readJson(paths.headFile(), HeadPointer.class);
        List<Path> commitFiles = store.list(paths.metadataDir()).stream()
                .filter(p -> p.getFileName().toString().matches("commit-\\d+\\.json"))
                .sorted((a, b) -> Long.compare(seqOf(a), seqOf(b)))
                .toList();

        Set<String> liveFiles = new HashSet<>();
        int maxSchemaVersion = 0;
        for (Path f : commitFiles) {
            CommitRecord rec = store.readJson(f, CommitRecord.class);
            SnapshotManifest m = rec.manifest();
            if (m.seq() > head.seq()) {
                // commit record exists but head never advanced -> aborted transaction
                abortOrphanCommit(rec);
                continue;
            }
            for (Long expiredSeq : m.expiredSnapshotSeqs()) {
                SnapshotManifest gone = state.snapshotsBySeq().remove(expiredSeq);
                if (gone != null) {
                    state.snapshotsById().remove(gone.snapshotId());
                }
            }
            state.snapshotsBySeq().put(m.seq(), m);
            state.snapshotsById().put(m.snapshotId(), m);
            if (rec.batch() != null) {
                state.committedBatches().put(rec.batch().batchId(), rec.batch());
            }
            liveFiles.addAll(m.dataFiles());
            maxSchemaVersion = Math.max(maxSchemaVersion, m.schemaVersion());
        }

        SnapshotManifest headManifest = state.snapshotsBySeq().get(head.seq());
        if (headManifest == null) {
            throw new IllegalStateException("head commit missing from journal: " + head.snapshotId());
        }
        state.setHeadSnapshotId(head.snapshotId());
        state.setNextSeq(head.seq() + 1);
        state.setNextOffset(headManifest.nextOffset());
        TableSchema schema = store.readJson(paths.schemaFile(maxSchemaVersion), TableSchema.class);
        state.setSchema(schema);
        state.setNextColumnId(schema.nextColumnId());

        // data files not referenced by any live snapshot are staging leftovers -> safe to remove
        int orphans = 0;
        for (Path dataFile : store.list(paths.dataDir())) {
            if (!liveFiles.contains(dataFile.getFileName().toString())) {
                store.delete(dataFile);
                orphans++;
            }
        }
        log.info("table[{}] recovered head={} seq={} offset={} schema=v{} liveSnapshots={} orphanFilesRemoved={}",
                name, head.snapshotId(), head.seq(), headManifest.nextOffset(),
                maxSchemaVersion, state.snapshotsBySeq().size(), orphans);
    }

    private void abortOrphanCommit(CommitRecord rec) {
        log.warn("table[{}] recovery: removing aborted commit seq={} snapshot={}",
                name, rec.seq(), rec.manifest().snapshotId());
        for (String file : rec.manifest().dataFiles()) {
            store.delete(paths.dataFile(file));
        }
        store.delete(paths.commitFile(rec.seq()));
    }

    private static long seqOf(Path commitFile) {
        String n = commitFile.getFileName().toString();
        return Long.parseLong(n.substring("commit-".length(), n.length() - ".json".length()));
    }

    // ------------------------------------------------------------- publish --

    /**
     * Durable publication order: commit record first, head pointer last.
     * Only on success is in-memory state updated, so any failure before this
     * returns has zero visible effect.
     */
    private void publish(CommitRecord rec, TableSchema schemaAtCommit, List<String> stagedFiles) {
        SnapshotManifest m = rec.manifest();
        store.writeJsonAtomically(paths.commitFile(m.seq()), rec);
        store.writeJsonAtomically(paths.headFile(), new HeadPointer(m.snapshotId(), m.seq()));

        // published durably; now update memory
        if (state.schema() == null || schemaAtCommit.version() >= state.schema().version()) {
            state.setSchema(schemaAtCommit);
            state.setNextColumnId(schemaAtCommit.nextColumnId());
        }
        for (Long expiredSeq : m.expiredSnapshotSeqs()) {
            SnapshotManifest gone = state.snapshotsBySeq().remove(expiredSeq);
            if (gone != null) {
                state.snapshotsById().remove(gone.snapshotId());
            }
        }
        state.snapshotsBySeq().put(m.seq(), m);
        state.snapshotsById().put(m.snapshotId(), m);
        if (rec.batch() != null) {
            state.committedBatches().put(rec.batch().batchId(), rec.batch());
        }
        state.setHeadSnapshotId(m.snapshotId());
        state.setNextSeq(m.seq() + 1);
        state.setNextOffset(m.nextOffset());
    }

    private String newDataFileName(long seq) {
        return "seq-" + seq + "-" + UUID.randomUUID().toString().substring(0, 8) + ".json";
    }

    private static String snapshotIdFor(long seq) {
        return "s" + seq;
    }

    // --------------------------------------------------------- row helpers --

    private SnapshotManifest headManifest() {
        return state.snapshotsById().get(state.headSnapshotId());
    }

    /**
     * Materializes the current logical table for a manifest: read every immutable
     * data file and resolve rows by primary key, keeping the highest file seq
     * (UPSERT semantics). Pure read of immutable files; safe under compaction.
     */
    private java.util.LinkedHashMap<Object, RowRecord> latestRowsByPk(TableSchema schema,
                                                                      SnapshotManifest manifest) {
        java.util.LinkedHashMap<Object, RowRecord> byPk = new java.util.LinkedHashMap<>();
        java.util.Map<Object, Long> seqByPk = new java.util.HashMap<>();
        List<Path> files = new ArrayList<>(manifest.dataFiles().stream().map(paths::dataFile).toList());
        files.sort((a, b) -> Long.compare(seqOfDataFile(a), seqOfDataFile(b)));
        for (Path f : files) {
            DataFilePayload payload = store.readJson(f, DataFilePayload.class);
            long fileSeq = payload.seq();
            for (RowRecord row : payload.rows()) {
                Object pk = pkKey(schema, row);
                Long seen = seqByPk.get(pk);
                if (seen == null || fileSeq >= seen) {
                    byPk.put(pk, row);
                    seqByPk.put(pk, fileSeq);
                }
            }
        }
        return byPk;
    }

    /**
     * Canonical primary-key value. JSON may materialize the same LONG as Integer or Long
     * depending on magnitude, so merge/conflict logic must compare normalized keys.
     */
    private Object pkKey(TableSchema schema, RowRecord row) {
        Object raw = row.valuesByColumnId().get(schema.primaryKeyColumnId());
        ColumnDef pkCol = schema.columns().stream()
                .filter(c -> c.id() == schema.primaryKeyColumnId()).findFirst().orElseThrow();
        return switch (pkCol.type()) {
            case LONG -> ((Number) raw).longValue();
            case DOUBLE -> ((Number) raw).doubleValue();
            case BOOLEAN -> raw;
            case STRING -> raw;
        };
    }

    private long seqOfDataFile(Path f) {
        String n = f.getFileName().toString();
        try {
            return Long.parseLong(n.substring("seq-".length(), n.indexOf('-', "seq-".length())));
        } catch (RuntimeException e) {
            return -1;
        }
    }

    private RowRecord normalizeRow(RowRecord row, TableSchema schema) {
        java.util.Map<Integer, Object> values = new java.util.LinkedHashMap<>();
        for (ColumnDef col : schema.columns()) {
            Object v = row.valuesByColumnId().get(col.id());
            if (v != null) {
                values.put(col.id(), coerce(col, v));
            }
        }
        return new RowRecord(values);
    }

    /** Returns null when the value is acceptable for the column, otherwise an error description. */
    private String checkType(ColumnDef col, Object v) {
        switch (col.type()) {
            case STRING -> {
                if (!(v instanceof String)) {
                    return "expected STRING but got " + v.getClass().getSimpleName();
                }
            }
            case BOOLEAN -> {
                if (!(v instanceof Boolean)) {
                    return "expected BOOLEAN but got " + v.getClass().getSimpleName();
                }
            }
            case LONG -> {
                if (v instanceof Integer || v instanceof Long) {
                    long n = ((Number) v).longValue();
                    if (v instanceof Integer && n != ((Integer) v).longValue()) {
                        return "integer value out of LONG range";
                    }
                } else {
                    return "expected LONG but got " + v.getClass().getSimpleName();
                }
            }
            case DOUBLE -> {
                if (!(v instanceof Number)) {
                    return "expected DOUBLE/NUMBER but got " + v.getClass().getSimpleName();
                }
            }
            default -> {
                return "unsupported type " + col.type();
            }
        }
        return null;
    }

    private Object coerce(ColumnDef col, Object v) {
        return switch (col.type()) {
            case LONG -> ((Number) v).longValue();
            case DOUBLE -> ((Number) v).doubleValue();
            default -> v;
        };
    }

    /** Stable hash of the request payload, used to detect same-id/different-payload replays. */
    private int requestSignature(BatchRequest request) {
        int result = 1;
        result = 31 * result + (request.mode() == null ? 0 : request.mode().hashCode());
        result = 31 * result + Long.hashCode(request.expectedOffset());
        for (RowRecord row : request.rows()) {
            result = 31 * result + (row == null || row.valuesByColumnId() == null
                    ? 0 : row.valuesByColumnId().toString().hashCode());
        }
        return result;
    }

    // ----------------------------------------------------------- writes --

    public BatchOutcome submit(BatchRequest request, Duration timeout) {
        String batchId = request == null ? null : request.batchId();
        if (request == null || batchId == null || batchId.isBlank()
                || request.rows() == null || request.rows().isEmpty() || request.mode() == null) {
            return reject(null, null, RejectReason.TYPE_MISMATCH,
                    "batch must carry batchId, mode and at least one row");
        }

        boolean locked;
        try {
            locked = lock.writeLock().tryLock(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return reject(batchId, null, RejectReason.BATCH_TIMEOUT, "interrupted while waiting for commit lock");
        }
        if (!locked) {
            log.warn("table[{}] batch={} REJECT reason=BATCH_TIMEOUT offset={} waitedMs={}",
                    name, batchId, request.expectedOffset(), timeout.toMillis());
            return BatchOutcome.rejected(RejectReason.BATCH_TIMEOUT,
                    "commit lock not acquired within " + timeout.toMillis() + "ms");
        }
        try {
            return doSubmit(request);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private BatchOutcome doSubmit(BatchRequest request) {
        String batchId = request.batchId();
        TableSchema schema = state.schema();
        long currentOffset = state.nextOffset();

        // --- idempotency: same batchId must be an identical replay ---
        CommittedBatch previous = state.committedBatches().get(batchId);
        int requestSignature = requestSignature(request);
        if (previous != null) {
            SnapshotManifest prevSnap = state.snapshotsById().get(previous.snapshotId());
            if (prevSnap != null && previous.expectedOffset() == request.expectedOffset()
                    && previous.mode() == request.mode()
                    && prevSnap.signatureSeen() == requestSignature) {
                log.info("table[{}] batch={} REPLAY idempotent snapshot={} offset={} (no new data written)",
                        name, batchId, previous.snapshotId(), state.nextOffset());
                return BatchOutcome.replayed(previous.snapshotId(), state.nextOffset());
            }
            log.warn("table[{}] batch={} REJECT reason=BATCH_ID_REUSE alreadyCommittedSnapshot={} offset={}",
                    name, batchId, previous.snapshotId(), currentOffset);
            return BatchOutcome.rejected(RejectReason.BATCH_ID_REUSE,
                    "batchId " + batchId + " already committed with a different payload/offset/mode");
        }

        // --- offset gate: strictly contiguous ingestion position ---
        if (request.expectedOffset() != currentOffset) {
            log.warn("table[{}] batch={} REJECT reason=INVALID_OFFSET expected={} actual={} snapshot={}",
                    name, batchId, currentOffset, request.expectedOffset(), state.headSnapshotId());
            return BatchOutcome.rejected(RejectReason.INVALID_OFFSET,
                    "expectedOffset " + request.expectedOffset() + " != committed offset " + currentOffset);
        }

        // --- schema/type validation (whole batch, before anything is staged) ---
        for (int i = 0; i < request.rows().size(); i++) {
            RowRecord row = request.rows().get(i);
            if (row == null || row.valuesByColumnId() == null) {
                return reject(batchId, row, RejectReason.TYPE_MISMATCH, "row " + i + " is empty");
            }
            for (ColumnDef col : schema.columns()) {
                Object v = row.valuesByColumnId().get(col.id());
                if (v == null) {
                    if (!col.nullable()) {
                        return reject(batchId, row, RejectReason.TYPE_MISMATCH,
                                "row " + i + " missing non-nullable column " + col.name() + "(id=" + col.id() + ")");
                    }
                    continue;
                }
                String typeError = checkType(col, v);
                if (typeError != null) {
                    return reject(batchId, row, RejectReason.TYPE_MISMATCH,
                            "row " + i + " column " + col.name() + "(id=" + col.id() + "): " + typeError);
                }
            }
            for (Integer unknownId : row.valuesByColumnId().keySet()) {
                if (schema.columns().stream().noneMatch(c -> c.id() == unknownId)) {
                    return reject(batchId, row, RejectReason.TYPE_MISMATCH,
                            "row " + i + " references unknown column id " + unknownId);
                }
            }
        }

        // --- primary key rules against the committed image ---
        java.util.Map<Object, RowRecord> committed = latestRowsByPk(schema, headManifest());
        java.util.Set<Object> pendingPks = new java.util.HashSet<>();
        for (int i = 0; i < request.rows().size(); i++) {
            RowRecord row = normalizeRow(request.rows().get(i), schema);
            Object pk = pkKey(schema, row);
            if (request.mode() == WriteMode.APPEND) {
                if (!pendingPks.add(pk)) {
                    return reject(batchId, row, RejectReason.PK_CONFLICT,
                            "row " + i + " duplicates primary key " + pk + " inside the same APPEND batch");
                }
                if (committed.containsKey(pk)) {
                    return reject(batchId, row, RejectReason.PK_CONFLICT,
                            "row " + i + " primary key " + pk + " already exists (APPEND does not overwrite)");
                }
            } else {
                pendingPks.add(pk); // UPSERT: later row in the same batch wins
            }
        }

        // --- stage + publish (atomic; failure leaves nothing visible) ---
        long seq = state.nextSeq();
        String fileName = null;
        try {
            List<RowRecord> normalized = request.rows().stream()
                    .map(r -> normalizeRow(r, schema)).toList();
            fileName = newDataFileName(seq);
            store.writeJsonAtomically(paths.dataFile(fileName),
                    new DataFilePayload(schema.version(), seq, normalized));

            SnapshotManifest parent = headManifest();
            List<String> dataFiles = new ArrayList<>(parent.dataFiles());
            dataFiles.add(fileName);
            long newOffset = currentOffset + 1;
            SnapshotManifest manifest = new SnapshotManifest(snapshotIdFor(seq), parent.seq(), seq,
                    System.currentTimeMillis(), schema.version(), newOffset, dataFiles,
                    "WRITE", "batch " + batchId + " (" + request.mode() + ", " + normalized.size() + " rows)",
                    parent.expiredSnapshotSeqs(), requestSignature);
            CommittedBatch batch = new CommittedBatch(batchId, request.expectedOffset(), newOffset,
                    manifest.snapshotId(), request.mode());
            publish(new CommitRecord(seq, manifest, batch), schema, List.of(fileName));
            log.info("table[{}] batch={} COMMITTED mode={} snapshot={} parent={} offset={}->{} rows={} file={}",
                    name, batchId, request.mode(), manifest.snapshotId(), parent.snapshotId(),
                    currentOffset, newOffset, normalized.size(), fileName);
            return BatchOutcome.committed(manifest.snapshotId(), newOffset);
        } catch (RuntimeException ex) {
            log.error("table[{}] batch={} FAILED during publish seq={} snapshot={} staged={}: {} "
                            + "(nothing visible; staged file will be reclaimed on next open)",
                    name, batchId, seq, snapshotIdFor(seq), fileName, ex.toString());
            return BatchOutcome.rejected(RejectReason.IO_FAILURE,
                    "commit failed atomically at seq " + seq + ": " + ex.getMessage());
        }
    }

    private BatchOutcome reject(String batchId, RowRecord row, RejectReason reason, String detail) {
        log.warn("table[{}] batch={} REJECT reason={} snapshot={} offset={} detail='{}'",
                name, batchId, reason, state.headSnapshotId(), state.nextOffset(), detail);
        return BatchOutcome.rejected(reason, detail);
    }

    // -------------------------------------------------------------- schema --

    public SchemaChangeOutcome evolve(SchemaChange change, Duration timeout) {
        if (change == null) {
            return schemaRejected("null schema change");
        }
        boolean locked;
        try {
            locked = lock.writeLock().tryLock(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return schemaRejected("interrupted while waiting for commit lock");
        }
        if (!locked) {
            log.warn("table[{}] schema REJECT reason=BATCH_TIMEOUT waitedMs={}", name, timeout.toMillis());
            return new SchemaChangeOutcome(false, state.schema().version(), null,
                    RejectReason.BATCH_TIMEOUT, "commit lock not acquired within " + timeout.toMillis() + "ms");
        }
        try {
            return doEvolve(change);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private SchemaChangeOutcome schemaRejected(String detail) {
        log.warn("table[{}] schema REJECT reason=SCHEMA_INCOMPATIBLE snapshot={} detail='{}'",
                name, state.headSnapshotId(), detail);
        return new SchemaChangeOutcome(false, state.schema() == null ? 0 : state.schema().version(),
                null, RejectReason.SCHEMA_INCOMPATIBLE, detail);
    }

    private SchemaChangeOutcome doEvolve(SchemaChange change) {
        TableSchema old = state.schema();
        List<ColumnDef> columns = new ArrayList<>(old.columns());
        List<String> names = columns.stream().map(ColumnDef::name).collect(Collectors.toCollection(ArrayList::new));

        if (change instanceof SchemaChange.AddColumn add) {
            if (add.name() == null || add.name().isBlank() || add.type() == null) {
                return schemaRejected("add-column requires name and type");
            }
            if (names.contains(add.name())) {
                return schemaRejected("add-column conflicts: name already exists: " + add.name());
            }
            // A non-nullable column can only be added when existing rows could default it -
            // we provide no backfill, so that would silently create invalid old rows.
            if (!add.nullable()) {
                SnapshotManifest head = headManifest();
                boolean tableHasData = head.dataFiles().stream()
                        .map(f -> store.readJson(paths.dataFile(f), DataFilePayload.class))
                        .anyMatch(p -> !p.rows().isEmpty());
                if (tableHasData) {
                    return schemaRejected("cannot add non-nullable column '" + add.name()
                            + "' to a non-empty table without a default/backfill");
                }
            }
            columns.add(new ColumnDef(old.nextColumnId(), add.name(), add.type(), add.nullable()));
        } else if (change instanceof SchemaChange.RenameColumn rename) {
            int idx = names.indexOf(rename.from());
            if (idx < 0) {
                return schemaRejected("rename source column does not exist: " + rename.from());
            }
            if (rename.to() == null || rename.to().isBlank()) {
                return schemaRejected("rename target name is blank");
            }
            if (names.contains(rename.to())) {
                return schemaRejected("rename target name already exists: " + rename.to());
            }
            ColumnDef c = columns.get(idx);
            columns.set(idx, new ColumnDef(c.id(), rename.to(), c.type(), c.nullable()));
        } else if (change instanceof SchemaChange.DropColumn drop) {
            int idx = names.indexOf(drop.name());
            if (idx < 0) {
                return schemaRejected("drop target column does not exist: " + drop.name());
            }
            if (columns.get(idx).id() == old.primaryKeyColumnId()) {
                return schemaRejected("cannot drop primary key column: " + drop.name());
            }
            columns.remove(idx);
        } else {
            return schemaRejected("unsupported schema change: " + change);
        }

        int newVersion = old.version() + 1;
        TableSchema next = new TableSchema(newVersion, old.nextColumnId(), old.primaryKeyColumnId(),
                List.copyOf(columns));
        long seq = state.nextSeq();
        SnapshotManifest parent = headManifest();
        SnapshotManifest manifest = new SnapshotManifest(snapshotIdFor(seq), parent.seq(), seq,
                System.currentTimeMillis(), newVersion, parent.nextOffset(), parent.dataFiles(),
                "SCHEMA", "schema v" + old.version() + " -> v" + newVersion + ": " + describe(change),
                parent.expiredSnapshotSeqs());
        try {
            store.writeJsonAtomically(paths.schemaFile(newVersion), next);
            publish(CommitRecord.of(manifest), next, List.of());
            log.info("table[{}] SCHEMA-EVOLVED {} -> v{} snapshot={} change='{}' columns={}",
                    name, old.version(), newVersion, manifest.snapshotId(), describe(change),
                    columns.stream().map(c -> c.name() + "#" + c.id()).toList());
            return new SchemaChangeOutcome(true, newVersion, manifest.snapshotId(), null, null);
        } catch (RuntimeException ex) {
            log.error("table[{}] schema change FAILED at seq={}: {} (nothing visible)", name, seq, ex.toString());
            return new SchemaChangeOutcome(false, old.version(), null, RejectReason.IO_FAILURE,
                    "schema commit failed: " + ex.getMessage());
        }
    }

    private String describe(SchemaChange change) {
        return switch (change) {
            case SchemaChange.AddColumn a -> "ADD " + a.name() + " " + a.type()
                    + (a.nullable() ? " NULL" : " NOT NULL");
            case SchemaChange.RenameColumn r -> "RENAME " + r.from() + " -> " + r.to();
            case SchemaChange.DropColumn d -> "DROP " + d.name();
        };
    }

    // ---------------------------------------------------------------- reads --

    public ReadOutcome readHead() {
        lock.readLock().lock();
        try {
            SnapshotManifest manifest = headManifest();
            TableSchema targetSchema = state.schema();
            return read(manifest, targetSchema, "HEAD");
        } finally {
            lock.readLock().unlock();
        }
    }

    public ReadOutcome readAt(String snapshotId) {
        lock.readLock().lock();
        try {
            SnapshotManifest manifest = state.snapshotsById().get(snapshotId);
            if (manifest == null) {
                throw new com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.LakehouseException(
                        RejectReason.UNKNOWN_SNAPSHOT,
                        "snapshot " + snapshotId + " does not exist or has expired in table " + name);
            }
            // A snapshot is always interpreted with its own schema: data written under a
            // newer schema is invisible to it (its file list is immutable), and columns
            // added later are projected as explained defaults (see projectRows).
            TableSchema snapshotSchema = store.readJson(paths.schemaFile(manifest.schemaVersion()),
                    TableSchema.class);
            return read(manifest, snapshotSchema, "AS_OF");
        } finally {
            lock.readLock().unlock();
        }
    }

    private ReadOutcome read(SnapshotManifest manifest, TableSchema projectionSchema, String mode) {
        java.util.LinkedHashMap<Object, RowRecord> merged = latestRowsByPk(projectionSchema, manifest);
        List<RowView> views = new ArrayList<>();
        for (RowRecord row : merged.values()) {
            views.add(projectRow(row, projectionSchema));
        }
        log.info("table[{}] READ {} snapshot={} schema=v{} rows={} files={} decision=upsert-merge-by-pk#{}",
                name, mode, manifest.snapshotId(), projectionSchema.version(), views.size(),
                manifest.dataFiles().size(), projectionSchema.primaryKeyColumnId());
        return new ReadOutcome(manifest.snapshotId(), projectionSchema.version(), views);
    }

    /**
     * Projects an id-keyed stored row onto the reading schema:
     * <ul>
     *   <li>FROM_DATA: value present in the row file;</li>
     *   <li>DEFAULT_NULL: column added after this row was written -> stable, explained null;</li>
     *   <li>columns dropped before this projection are simply absent.</li>
     * </ul>
     * Rows are keyed by stable column id, so rename never misaligns data.
     */
    private RowView projectRow(RowRecord stored, TableSchema projection) {
        java.util.Map<String, Object> values = new java.util.LinkedHashMap<>();
        java.util.Map<String, String> sources = new java.util.LinkedHashMap<>();
        for (ColumnDef col : projection.columns()) {
            Object v = stored.valuesByColumnId().get(col.id());
            if (stored.valuesByColumnId().containsKey(col.id())) {
                values.put(col.name(), v == null ? null : coerce(col, v));
                sources.put(col.name(), "FROM_DATA");
            } else {
                values.put(col.name(), null);
                sources.put(col.name(), "DEFAULT_NULL"); // column added after the row was written
            }
        }
        return new RowView(values, sources);
    }

    // --------------------------------------------------------- maintenance --

    public MaintenanceOutcome compact(Duration timeout) {
        return runMaintenance("COMPACT", timeout, false, 0);
    }

    public MaintenanceOutcome expireSnapshots(int retainCount, Duration timeout) {
        if (retainCount < 1) {
            throw new com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.LakehouseException(
                    RejectReason.SCHEMA_INCOMPATIBLE, "retainCount must be >= 1 (head is always retained)");
        }
        return runMaintenance("EXPIRE", timeout, true, retainCount);
    }

    private MaintenanceOutcome runMaintenance(String op, Duration timeout, boolean expire, int retainCount) {
        boolean locked;
        try {
            locked = lock.writeLock().tryLock(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return maintenanceRejected(op, "interrupted while waiting");
        }
        if (!locked) {
            log.warn("table[{}] {} REJECT reason=MAINTENANCE_BUSY waitedMs={}", name, op, timeout.toMillis());
            return maintenanceRejected(op, "maintenance lock not acquired within " + timeout.toMillis() + "ms");
        }
        try {
            return expire ? doExpire(retainCount, timeout) : doCompact();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private MaintenanceOutcome maintenanceRejected(String op, String detail) {
        return new MaintenanceOutcome(false, op, state.headSnapshotId(), 0, 0, List.of(),
                RejectReason.MAINTENANCE_BUSY, detail);
    }

    private MaintenanceOutcome doCompact() {
        TableSchema schema = state.schema();
        SnapshotManifest parent = headManifest();
        int filesBefore = parent.dataFiles().size();
        if (filesBefore <= 1) {
            log.info("table[{}] COMPACT noop snapshot={} files={} decision=nothing-to-compact",
                    name, parent.snapshotId(), filesBefore);
            return new MaintenanceOutcome(true, "COMPACT", parent.snapshotId(), filesBefore, filesBefore,
                    List.of(), null, "nothing to compact");
        }

        long seq = state.nextSeq();
        String compactedFile = newDataFileName(seq);
        try {
            java.util.LinkedHashMap<Object, RowRecord> merged = latestRowsByPk(schema, parent);
            List<RowRecord> rows = new ArrayList<>(merged.values());
            store.writeJsonAtomically(paths.dataFile(compactedFile),
                    new DataFilePayload(schema.version(), seq, rows));

            SnapshotManifest manifest = new SnapshotManifest(snapshotIdFor(seq), parent.seq(), seq,
                    System.currentTimeMillis(), schema.version(), parent.nextOffset(),
                    List.of(compactedFile), "COMPACT",
                    filesBefore + " files -> 1, " + rows.size() + " live rows (upsert-merged)",
                    parent.expiredSnapshotSeqs());
            publish(CommitRecord.of(manifest), schema, List.of(compactedFile));
            log.info("table[{}] COMPACT DONE snapshot={} parent={} files={}->{} rows={} newFile={} "
                            + "decision=old-snapshots-still-readable",
                    name, manifest.snapshotId(), parent.snapshotId(), filesBefore, 1, rows.size(),
                    compactedFile);
            return new MaintenanceOutcome(true, "COMPACT", manifest.snapshotId(), filesBefore, 1,
                    List.of(), null, null);
        } catch (RuntimeException ex) {
            store.delete(paths.dataFile(compactedFile));
            log.error("table[{}] COMPACT FAILED seq={}: {} (head {} untouched; existing snapshots intact)",
                    name, seq, ex.toString(), parent.snapshotId());
            return new MaintenanceOutcome(false, "COMPACT", parent.snapshotId(), filesBefore, filesBefore,
                    List.of(), RejectReason.IO_FAILURE, "compaction failed, no snapshot changed: " + ex.getMessage());
        }
    }

    private MaintenanceOutcome doExpire(int retainCount, Duration timeout) {
        List<SnapshotManifest> live = state.snapshotsInOrder();
        // Boundary markers (EXPIRE/COMPACT/SCHEMA snapshots that carry no data of their own beyond
        // a reference) are part of the chain; the newest one is always the head. We keep the last
        // `retainCount` snapshots and expire the rest, which deterministically leaves the head intact.
        int keep = Math.min(Math.max(retainCount, 1), live.size());
        List<SnapshotManifest> toExpire = live.subList(0, live.size() - keep);
        if (toExpire.isEmpty()) {
            log.info("table[{}] EXPIRE noop snapshot={} retain={} decision=fewer-snapshots-than-retain",
                    name, state.headSnapshotId(), retainCount);
            return new MaintenanceOutcome(true, "EXPIRE", state.headSnapshotId(), 0, 0, List.of(),
                    null, "nothing to expire");
        }

        // Files eligible for physical deletion: referenced only by snapshots being expired.
        Set<String> retainedFiles = new HashSet<>();
        live.subList(live.size() - keep, live.size())
                .forEach(m -> retainedFiles.addAll(m.dataFiles()));
        Set<String> expiredFiles = new HashSet<>();
        for (SnapshotManifest m : toExpire) {
            expiredFiles.addAll(m.dataFiles());
        }
        expiredFiles.removeAll(retainedFiles);

        long seq = state.nextSeq();
        List<Long> expiredSeqs = new ArrayList<>(headManifest().expiredSnapshotSeqs());
        toExpire.forEach(m -> expiredSeqs.add(m.seq()));
        SnapshotManifest parent = headManifest();
        SnapshotManifest manifest = new SnapshotManifest(snapshotIdFor(seq), parent.seq(), seq,
                System.currentTimeMillis(), parent.schemaVersion(), parent.nextOffset(),
                parent.dataFiles(), "EXPIRE",
                "expired " + toExpire.size() + " snapshot(s), retained " + keep, expiredSeqs);
        try {
            // Publish the new boundary first. If we crash before physical deletion, the
            // expired snapshots are already unreachable; on recovery the orphan files are
            // reclaimed deterministically - old data never resurfaces.
            publish(CommitRecord.of(manifest), state.schema(), List.of());
            List<String> removed = new ArrayList<>();
            List<String> deferred = new ArrayList<>();
            for (String f : expiredFiles) {
                try {
                    store.delete(paths.dataFile(f));
                    removed.add(f);
                } catch (RuntimeException deleteError) {
                    deferred.add(f);
                    log.warn("table[{}] EXPIRE physical delete deferred for {} (unreachable, reclaimed on open): {}",
                            name, f, deleteError.toString());
                }
            }
            for (SnapshotManifest m : toExpire) {
                try {
                    store.delete(paths.commitFile(m.seq()));
                } catch (RuntimeException deleteError) {
                    log.warn("table[{}] EXPIRE commit journal delete deferred for seq={}: {}",
                            name, m.seq(), deleteError.toString());
                }
            }
            log.info("table[{}] EXPIRE DONE snapshot={} expiredSnapshots={} retained={} deletedFiles={} "
                            + "deferredFiles={} boundaryPublishedFirst=true timeoutMs={}",
                    name, manifest.snapshotId(),
                    toExpire.stream().map(SnapshotManifest::snapshotId).toList(),
                    live.subList(live.size() - keep, live.size()).stream()
                            .map(SnapshotManifest::snapshotId).toList(),
                    removed, deferred, timeout.toMillis());
            return new MaintenanceOutcome(true, "EXPIRE", manifest.snapshotId(),
                    parent.dataFiles().size(), parent.dataFiles().size(), removed, null,
                    deferred.isEmpty() ? null : "physical deletion deferred for " + deferred.size() + " file(s)");
        } catch (RuntimeException ex) {
            log.error("table[{}] EXPIRE failed seq={}: {} (already expired snapshots stay expired; "
                            + "physical files reclaimed on next open)", name, seq, ex.toString());
            return new MaintenanceOutcome(false, "EXPIRE", parent.snapshotId(), 0, 0, List.of(),
                    RejectReason.IO_FAILURE, "expire failed: " + ex.getMessage());
        }
    }
}
