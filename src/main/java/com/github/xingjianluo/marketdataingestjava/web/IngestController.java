package com.github.xingjianluo.marketdataingestjava.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestjava.model.AggregateView;
import com.github.xingjianluo.marketdataingestjava.model.IngestResult;
import com.github.xingjianluo.marketdataingestjava.pipeline.IngestPipeline;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayVerificationResult;
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

    @PostMapping("/restore")
    public String restore() {
        pipeline.restore();
        return "ok";
    }

    /**
     * 在运行中的链路上发起整体重放核对：从干净空白状态重放已收下的全部历史事件，
     * 逐条比对首次结论，返回 总数/一致/不一致 统计与不一致明细。
     * 请求体可省略；携带 resumeFromPosition 时仅允许干净起点（0），
     * 靠前（回退）或靠后（跳跃）都会得到 REJECTED_OFFSET_REGRESSION 的明确拒绝。
     */
    @PostMapping("/replay/verify")
    public ReplayVerificationResult verifyReplay(
            @RequestBody(required = false) ReplayVerifyRequest request) {
        Long resumeFromPosition = request == null ? null : request.getResumeFromPosition();
        ReplayVerificationResult result = pipeline.verifyReplay(resumeFromPosition);
        log.info("[入口] 整体重放核对发起: status={} {}", result.status(),
                result.report() != null ? result.report().summary() : "reason='" + result.reason() + "'");
        return result;
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
