package com.github.xingjianluo.marketdataingestjava.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

/**
 * 基于本地 JSON 文件的原子快照存储：先写同目录临时文件，再原子 move 覆盖。
 * 崩溃时要么保留旧快照、要么得到完整新快照，不会读到半成品。
 */
public class SnapshotStore {

    private static final Logger log = LoggerFactory.getLogger(SnapshotStore.class);

    private final Path file;
    private final ObjectMapper mapper;

    public SnapshotStore(Path file, ObjectMapper mapper) {
        this.file = file;
        this.mapper = mapper.copy()
                .enable(SerializationFeature.INDENT_OUTPUT);
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
        } catch (IOException e) {
            throw new UncheckedIOException("无法创建快照目录: " + file, e);
        }
    }

    public synchronized void save(EngineSnapshot snapshot) {
        try {
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            byte[] bytes = mapper.writeValueAsBytes(snapshot);
            Files.write(tmp, bytes, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            try {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicUnsupported) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
            log.debug("快照已保存 file={} bytes={}", file, bytes.length);
        } catch (IOException e) {
            throw new UncheckedIOException("保存快照失败: " + file, e);
        }
    }

    public synchronized Optional<EngineSnapshot> load() {
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try {
            byte[] bytes = Files.readAllBytes(file);
            if (bytes.length == 0) {
                return Optional.empty();
            }
            return Optional.of(mapper.readValue(bytes, EngineSnapshot.class));
        } catch (IOException e) {
            throw new UncheckedIOException("读取快照失败（快照可能损坏，拒绝在不确定状态下启动）: " + file, e);
        }
    }

    /** 删除快照文件（全量重放前清空状态时使用）。 */
    public synchronized void reset() {
        try {
            Files.deleteIfExists(file);
            Files.deleteIfExists(file.resolveSibling(file.getFileName() + ".tmp"));
        } catch (IOException e) {
            throw new UncheckedIOException("重置快照失败: " + file, e);
        }
    }

    public Path file() {
        return file;
    }
}
