package com.github.xingjianluo.marketdataingestjava.state;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * 基于本地 JSON 文件 + 原子移动（tmp -> target, ATOMIC_REPLACE 不可用时退化为替换）
 * 的快照仓库。读取不到文件视为“尚无快照”，返回 null。
 * 全程只使用本地文件系统，不依赖任何外部服务。
 */
public class FileSnapshotRepository implements SnapshotRepository {

    private final Path location;
    private final ObjectMapper objectMapper;

    public FileSnapshotRepository(Path location, ObjectMapper objectMapper) {
        this.location = location;
        this.objectMapper = objectMapper;
    }

    @Override
    public void save(PipelineState state) {
        try {
            Path parent = location.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = location.resolveSibling(location.getFileName() + ".tmp");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), state);
            try {
                Files.move(tmp, location, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicNotSupported) {
                Files.move(tmp, location, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist snapshot to " + location, e);
        }
    }

    @Override
    public PipelineState load() {
        if (!Files.exists(location)) {
            return null;
        }
        try {
            return objectMapper.readValue(location.toFile(), PipelineState.class);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read snapshot from " + location, e);
        }
    }
}
