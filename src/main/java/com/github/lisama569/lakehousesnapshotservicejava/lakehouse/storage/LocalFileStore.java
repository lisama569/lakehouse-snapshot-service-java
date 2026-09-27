package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Local-disk storage with atomic write-and-rename semantics.
 *
 * <p>Every metadata or data file is written to a unique temp file in the same
 * directory and then atomically moved into place, so a crash either leaves the
 * old file intact or the new file fully present - never a half-written file.
 *
 * <p>{@link #failureHook} is a test-only seam used to simulate a crash/failure
 * at a specific point of a commit.
 */
public class LocalFileStore {

    private final Path root;
    private final ObjectMapper mapper;

    /** Test-only: if it returns true for a target path, the write aborts before the atomic rename. */
    public volatile Predicate<Path> failureHook = p -> false;

    public LocalFileStore(Path root, ObjectMapper mapper) {
        this.root = root;
        this.mapper = mapper;
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create store root " + root, e);
        }
    }

    public Path root() {
        return root;
    }

    public void ensureDirectory(Path dir) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create directory " + dir, e);
        }
    }

    public boolean exists(Path file) {
        return Files.exists(file);
    }

    /** Atomically write JSON payload to {@code target}. */
    public void writeJsonAtomically(Path target, Object payload) {
        ensureDirectory(target.getParent());
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp-" + System.nanoTime());
        try {
            mapper.writeValue(tmp.toFile(), payload);
            if (failureHook.test(target)) {
                // Simulate a crash/IO failure after data is staged but before it is published.
                Files.deleteIfExists(tmp);
                throw new IOException("injected failure before atomic publish of " + target);
            }
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException ignored) {
                // best effort
            }
            throw new UncheckedIOException("atomic write failed for " + target, e);
        }
    }

    public <T> T readJson(Path file, Class<T> type) {
        try {
            return mapper.readValue(file.toFile(), type);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + file, e);
        }
    }

    public void delete(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot delete " + file, e);
        }
    }

    public List<Path> list(Path dir) {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> s = Files.list(dir)) {
            List<Path> out = new ArrayList<>();
            s.forEach(out::add);
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot list " + dir, e);
        }
    }
}
