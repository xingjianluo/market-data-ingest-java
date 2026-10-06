package com.github.xingjianluo.marketdataingestjava.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestjava.model.AggregateView;
import com.github.xingjianluo.marketdataingestjava.model.IngestResult;
import com.github.xingjianluo.marketdataingestjava.pipeline.IngestPipeline;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayReport;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

    public IngestController(IngestPipeline pipeline, ObjectMapper objectMapper) {
        this.pipeline = pipeline;
        this.objectMapper = objectMapper;
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

    /**
     * 在运行中的链路上发起整体重放核对：把已收下的全部历史事件（摄取流水账）
     * 从一套与当前内存完全无关的空白状态完整重放，逐条比对首次结论，
     * 返回统计（总数/一致/不一致）与不一致明细。
     * 核对只读流水账、不触碰链路状态，可随时、可并发发起。
     */
    @PostMapping("/replay-verify")
    public ReplayReport replayVerify() {
        ReplayReport report = ReplayVerifier.forPipeline(pipeline).verify(pipeline.journal());
        log.info("[重放核对] HTTP 发起完成: {}", report.summary());
        return report;
    }

    @PostMapping("/restore")
    public String restore() {
        pipeline.restore();
        return "ok";
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
