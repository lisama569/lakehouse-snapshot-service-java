package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.CommittedBatch;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.SnapshotManifest;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.TableSchema;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory authoritative state of an open table, reconstructed from disk.
 * Mutation is serialized by the table's write lock; snapshots are immutable.
 */
public class TableState {

    private volatile TableSchema schema;
    private volatile int nextColumnId = 1;
    private volatile long nextSeq = 1;
    private volatile long nextOffset;
    private volatile String headSnapshotId;
    private final Map<Long, SnapshotManifest> snapshotsBySeq = new ConcurrentHashMap<>();
    private final Map<String, SnapshotManifest> snapshotsById = new ConcurrentHashMap<>();
    private final Map<String, CommittedBatch> committedBatches = new ConcurrentHashMap<>();

    public TableState() {
    }

    public TableSchema schema() {
        return schema;
    }

    public void setSchema(TableSchema schema) {
        this.schema = schema;
    }

    public int nextColumnId() {
        return nextColumnId;
    }

    public void setNextColumnId(int nextColumnId) {
        this.nextColumnId = nextColumnId;
    }

    public long nextSeq() {
        return nextSeq;
    }

    public void setNextSeq(long nextSeq) {
        this.nextSeq = nextSeq;
    }

    public long nextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public String headSnapshotId() {
        return headSnapshotId;
    }

    public void setHeadSnapshotId(String headSnapshotId) {
        this.headSnapshotId = headSnapshotId;
    }

    public Map<Long, SnapshotManifest> snapshotsBySeq() {
        return snapshotsBySeq;
    }

    public Map<String, SnapshotManifest> snapshotsById() {
        return snapshotsById;
    }

    public Map<String, CommittedBatch> committedBatches() {
        return committedBatches;
    }

    /** Live snapshots in chronological order (expired snapshots are not present). */
    public List<SnapshotManifest> snapshotsInOrder() {
        return snapshotsBySeq.keySet().stream().sorted().map(snapshotsBySeq::get).toList();
    }
}
