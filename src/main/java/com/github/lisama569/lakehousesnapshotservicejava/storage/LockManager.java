package com.github.lisama569.lakehousesnapshotservicejava.storage;

import com.github.lisama569.lakehousesnapshotservicejava.exception.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.model.RejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 每张表一把提交锁：
 * <ul>
 *   <li>进程内用公平的 {@link ReentrantLock} 串行化同表的所有提交（写 / 合并 / 清理 / 演进）；</li>
 *   <li>进程间通过 lock 文件上的 {@link FileChannel#tryLock} 互斥，支持多 JVM 场景；</li>
 *   <li>提供 tryLock(timeout) 超时边界，超时按调用方给定的错误码拒绝
 *       （写入为 BATCH_TIMEOUT，维护为 MAINTENANCE_TIMEOUT）。</li>
 * </ul>
 */
public class LockManager {

    private static final Logger log = LoggerFactory.getLogger(LockManager.class);

    private static final class TableLock {
        private final ReentrantLock jvmLock = new ReentrantLock(true);
        private FileChannel channel;
        private FileLock fileLock;
    }

    private final Map<String, TableLock> locks = new ConcurrentHashMap<>();

    public LockManager() {
    }

    /**
     * 获取表级提交锁，返回值必须 try-with-resources 关闭。
     *
     * @param timeoutMs 等待超时；&lt;=0 表示不等待
     * @param reason    超时 / 中断时抛出的错误码
     */
    public AutoCloseable acquire(String table, Path lockFile, long timeoutMs, RejectReason reason) {
        TableLock lock = locks.computeIfAbsent(table, k -> new TableLock());
        boolean jvmHeld = false;
        try {
            jvmHeld = lock.jvmLock.tryLock(Math.max(timeoutMs, 0L), TimeUnit.MILLISECONDS);
            if (!jvmHeld) {
                log.warn("[lakehouse] lock-timeout stage=jvm table={} timeoutMs={} reason={}",
                        table, timeoutMs, reason);
                throw new LakehouseException(reason,
                        "等待提交锁超时 table=" + table + " timeoutMs=" + timeoutMs);
            }
            Files.createDirectories(lockFile.toAbsolutePath().getParent());
            lock.channel = FileChannel.open(lockFile,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            long deadline = System.currentTimeMillis() + Math.max(timeoutMs, 0L);
            FileLock fl = tryFileLockUntilDeadline(lock.channel, deadline);
            if (fl == null) {
                lock.channel.close();
                lock.channel = null;
                lock.jvmLock.unlock();
                log.warn("[lakehouse] lock-timeout stage=file table={} timeoutMs={} reason={}",
                        table, timeoutMs, reason);
                throw new LakehouseException(reason,
                        "等待文件提交锁超时 table=" + table + " timeoutMs=" + timeoutMs);
            }
            lock.fileLock = fl;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (jvmHeld) {
                lock.jvmLock.unlock();
            }
            throw new LakehouseException(reason, "等待提交锁被中断 table=" + table);
        } catch (IOException e) {
            if (jvmHeld) {
                lock.jvmLock.unlock();
            }
            throw new IllegalStateException("无法打开锁文件: " + lockFile, e);
        }

        return () -> {
            try {
                if (lock.fileLock != null) {
                    lock.fileLock.release();
                    lock.fileLock = null;
                }
            } catch (IOException e) {
                log.warn("[lakehouse] 释放文件锁失败 table={} reason={}", table, e.getMessage());
            } finally {
                if (lock.channel != null) {
                    try {
                        lock.channel.close();
                    } catch (IOException ignored) {
                        // 关闭失败不影响后续重新获取
                    }
                    lock.channel = null;
                }
                lock.jvmLock.unlock();
            }
        };
    }

    private FileLock tryFileLockUntilDeadline(FileChannel channel, long deadline) throws InterruptedException {
        while (true) {
            try {
                FileLock fl = channel.tryLock();
                if (fl != null) {
                    return fl;
                }
            } catch (OverlappingFileLockException e) {
                // 本 JVM 内同 channel 已持锁（理论上被 jvmLock 排除），按忙等处理。
            } catch (IOException e) {
                return null;
            }
            if (System.currentTimeMillis() >= deadline) {
                return null;
            }
            Thread.sleep(10);
        }
    }
}
