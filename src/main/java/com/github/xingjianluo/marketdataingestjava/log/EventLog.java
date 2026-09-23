package com.github.xingjianluo.marketdataingestjava.log;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestjava.protocol.IngestOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 本地 JSONL 事件日志：每条 {@link LoggedRecord} 占一行，只追加，用于全量重放与结论比对。
 *
 * <p>行格式：{@code {"recordedAt":..., "raw":<原始信封JSON字符串>, "outcome":<判定结论对象>}}。
 * 原始信封以字符串字段保存可完整还原（含版本/字段缺失等非法输入）；
 * 文件对象锁保证多线程并发追加不交错。
 */
public class EventLog {

    private static final Logger log = LoggerFactory.getLogger(EventLog.class);

    private final Path file;
    private final ObjectMapper mapper;
    private final Object lock = new Object();

    public EventLog(Path file) {
        this(file, new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL));
    }

    public EventLog(Path file, ObjectMapper mapper) {
        this.file = file;
        this.mapper = mapper;
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
        } catch (IOException e) {
            throw new UncheckedIOException("无法创建事件日志目录: " + file, e);
        }
    }

    /** 追加一条原始信封及其判定结论。 */
    public void append(String rawEnvelope, IngestOutcome outcome) {
        append(rawEnvelope, outcome, System.currentTimeMillis());
    }

    /** 显式指定记录时间，便于测试得到确定的日志内容。 */
    public void append(String rawEnvelope, IngestOutcome outcome, long recordedAt) {
        LoggedRecord record = new LoggedRecord(rawEnvelope, outcome, recordedAt);
        String line;
        try {
            line = mapper.writeValueAsString(record);
        } catch (Exception e) {
            throw new IllegalStateException("事件日志序列化失败", e);
        }
        synchronized (lock) {
            try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                writer.write(line);
                writer.newLine();
            } catch (IOException e) {
                throw new UncheckedIOException("写入事件日志失败: " + file, e);
            }
        }
        log.debug("事件日志追加 file={} bytes={}", file, line.length());
    }

    /** 读取全部记录，按追加顺序返回。损坏行被跳过并告警，不影响其余记录重放。 */
    public List<LoggedRecord> readRecords() {
        synchronized (lock) {
            if (!Files.exists(file)) {
                return List.of();
            }
            List<LoggedRecord> records = new ArrayList<>();
            int lineNo = 0;
            List<String> lines;
            try {
                lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException("读取事件日志失败: " + file, e);
            }
            for (String line : lines) {
                lineNo++;
                if (line.isBlank()) {
                    continue;
                }
                try {
                    records.add(mapper.readValue(line, LoggedRecord.class));
                } catch (Exception e) {
                    log.warn("跳过损坏的事件日志行 line={} content={}", lineNo, truncate(line));
                }
            }
            return records;
        }
    }

    /** 只取原始信封，按追加顺序返回。 */
    public List<String> readAll() {
        return readRecords().stream().map(LoggedRecord::raw).toList();
    }

    public Path file() {
        return file;
    }

    private static String truncate(String s) {
        return s.length() <= 80 ? s : s.substring(0, 80) + "...";
    }
}
