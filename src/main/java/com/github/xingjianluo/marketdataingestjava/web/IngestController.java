package com.github.xingjianluo.marketdataingestjava.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestjava.model.AggregateView;
import com.github.xingjianluo.marketdataingestjava.model.IngestResult;
import com.github.xingjianluo.marketdataingestjava.pipeline.IngestPipeline;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayRefusedException;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayReport;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 本地摄取、查询、快照与恢复的 HTTP 入口。
 * 事件逐条进入管道；当请求携带 expectedOffsets 时按“断点续处理/重放”语义严格校验位点。
 */
@RestController
@RequestMapping("/ingest")
public class IngestController {

    private static final Logger log = LoggerFactory.getLogger(IngestController.class);

    private final IngestPipeline pipeline;
    private final ObjectMapper objectMapper;
    private final ReplayService replayService;

    public IngestController(IngestPipeline pipeline, ObjectMapper objectMapper,
                            ReplayService replayService) {
        this.pipeline = pipeline;
        this.objectMapper = objectMapper;
        this.replayService = replayService;
    }

    /** 接收一批原始事件（多源、可并发调用）。 */
    @PostMapping("/events")
    public List<IngestResult> ingest(@RequestBody EventRequest request) {
        Map<String, Long> expected = request.getExpectedOffsets() == null
                ? Map.of() : request.getExpectedOffsets();
        List<IngestResult> results = new ArrayList<>(request.getEvents().size());
        for (String raw : request.getEvents()) {
            // 仅用于路由续处理校验；解析失败/缺 source 时交给管道给出 INVALID 结论
            String source = peekSource(raw);
            if (source != null && expected.containsKey(source)) {
                results.add(pipeline.resumeIngest(raw, expected));
            } else {
                results.add(pipeline.ingest(raw));
            }
        }
        log.info("[入口] 批量处理完成 size={} accepted={} duplicate={} invalid={} late={} regression={}",
                results.size(),
                results.stream().filter(r -> r.status().name().equals("ACCEPTED")).count(),
                results.stream().filter(r -> r.status().name().equals("DUPLICATE")).count(),
                results.stream().filter(r -> r.status().name().equals("REJECTED_INVALID")).count(),
                results.stream().filter(r -> r.status().name().equals("REJECTED_LATE")).count(),
                results.stream().filter(r -> r.status().name().equals("REJECTED_OFFSET_REGRESSION")).count());
        return results;
    }

    @GetMapping("/aggregates")
    public List<AggregateView> aggregates() {
        return pipeline.aggregates();
    }

    @PostMapping("/snapshot")
    public String snapshot() {
        pipeline.snapshot();
        return "ok";
    }

    @PostMapping("/restore")
    public String restore() {
        pipeline.restore();
        return "ok";
    }

    /**
     * 从干净空白状态把日志中的原始事件整体重放一遍并逐条核对结论。
     * 可选 body：{"startOffsets": {"src": -1}}；干净状态只接受 -1，否则 400 明确拒绝。
     */
    @PostMapping("/replay")
    public ReplayReport replay(@org.springframework.web.bind.annotation.RequestBody(required = false)
                               ReplayRequest request) {
        try {
            ReplayReport report = replayService.replayFromScratch(
                    request == null ? Map.of() : request.getStartOffsets());
            log.info("[入口] 整体重放核对完成 replayed={} matched={} mismatched={} aggregatesMatch={}",
                    report.totalReplayed(), report.matched(), report.mismatched(),
                    report.aggregatesMatch());
            return report;
        } catch (ReplayRefusedException ex) {
            log.warn("[入口] 整体重放被拒绝: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        }
    }

    private String peekSource(String raw) {
        try {
            JsonNode node = objectMapper.readTree(raw);
            JsonNode source = node.get("source");
            return source != null && source.isTextual() ? source.asText() : null;
        } catch (Exception e) {
            return null;
        }
    }
}
