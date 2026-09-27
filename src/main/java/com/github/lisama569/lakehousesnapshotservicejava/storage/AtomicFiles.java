package com.github.lisama569.lakehousesnapshotservicejava.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

/** 本地文件系统的原子写 / 移动工具，保证状态与数据文件提交的原子性。 */
public final class AtomicFiles {

    private static final Logger log = LoggerFactory.getLogger(AtomicFiles.class);

    private AtomicFiles() {
    }

    /**
     * 以临时文件 + 原子 move 的方式写出 UTF-8 文本。
     * 崩溃时只会留下 {@code *.tmp} 临时文件，可由后续操作回收，绝不会出现半成品目标文件。
     */
    public static void writeAtomically(Path target, String content) throws IOException {
        Path parent = target.toAbsolutePath().getParent();
        Files.createDirectories(parent);
        Path tmp = Files.createTempFile(parent, target.getFileName().toString(), ".tmp");
        try {
            Files.writeString(tmp, content, StandardCharsets.UTF_8);
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                // 极端文件系统降级：非原子 move 仍在同目录下，配合 tmp 回收保证不产生半成品状态文件。
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            deleteIfExistsQuietly(tmp);
            throw e;
        }
    }

    /**
     * 将临时数据文件原子发布为正式数据文件。同目录 move，崩溃后未发布的文件留在 staging 中可回收。
     */
    public static void publish(Path stagingFile, Path dataFile) throws IOException {
        Files.createDirectories(dataFile.toAbsolutePath().getParent());
        try {
            Files.move(stagingFile, dataFile,
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(stagingFile, dataFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public static void deleteIfExistsQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.warn("[lakehouse] 删除临时文件失败 path={} reason={}", path, e.getMessage());
        }
    }

    /** 删除文件列表，返回成功删除的文件；失败不影响其它文件。 */
    public static void deleteAllQuietly(List<Path> paths) {
        for (Path p : paths) {
            deleteIfExistsQuietly(p);
        }
    }

    /** 清空 staging 目录中未发布的临时文件（上次进程崩溃的残留物）。 */
    public static void reapStaging(Path stagingDir) {
        if (!Files.isDirectory(stagingDir)) {
            return;
        }
        try {
            Files.walkFileTree(stagingDir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    deleteIfExistsQuietly(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    if (!dir.equals(stagingDir)) {
                        deleteIfExistsQuietly(dir);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            log.warn("[lakehouse] 回收 staging 失败 dir={} reason={}", stagingDir, e.getMessage());
        }
    }
}
