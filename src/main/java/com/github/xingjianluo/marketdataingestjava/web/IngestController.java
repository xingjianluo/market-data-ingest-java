package com.github.xingjianluo.marketdataingestjava.web;

import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationKey;
import com.github.xingjianluo.marketdataingestjava.aggregate.WindowAggregation;
import com.github.xingjianluo.marketdataingestjava.engine.IngestEngine;
import com.github.xingjianluo.marketdataingestjava.protocol.IngestOutcome;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地 HTTP 接入：
 * <ul>
 *   <li>POST /api/ingest：提交一条原始事件信封，返回带明确依据的判定；
 *       被拒绝时以 422 返回（原因仍是结构化的 {@link IngestOutcome}），重复/超期/非法/位点问题可区分；</li>
 *   <li>GET /api/aggregations：查询当前聚合结果与位点/水位线；</li>
 *   <li>POST /api/replay：触发全量重放并返回对账结论。</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class IngestController {

    private final IngestEngine engine;
    private final ReplayService replayService;

    public IngestController(IngestEngine engine, ReplayService replayService) {
        this.engine = engine;
        this.replayService = replayService;
    }

    @PostMapping("/ingest")
    public ResponseEntity<IngestOutcome> ingest(@RequestBody String rawJson) {
        IngestOutcome outcome = engine.ingest(rawJson);
        // 拒绝不是“服务错误”：返回 422 便于调用方区分，同时响应体给出互斥原因与判定依据
        HttpStatus status = outcome.accepted() ? HttpStatus.OK : HttpStatus.UNPROCESSABLE_ENTITY;
        return ResponseEntity.status(status).body(outcome);
    }

    @GetMapping("/aggregations")
    public Map<String, Object> aggregations() {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("watermark", engine.watermark());
        view.put("cursors", engine.cursors());
        List<Map<String, Object>> windows = new ArrayList<>();
        for (Map.Entry<AggregationKey, WindowAggregation> e : engine.aggregations().entrySet()) {
            WindowAggregation a = e.getValue();
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("symbol", a.symbol());
            row.put("windowStart", a.windowStart());
            row.put("open", a.open());
            row.put("high", a.high());
            row.put("low", a.low());
            row.put("close", a.close());
            row.put("totalVolume", a.totalVolume());
            row.put("tradeCount", a.tradeCount());
            row.put("buyVolume", a.buyVolume());
            row.put("sellVolume", a.sellVolume());
            row.put("unknownVolume", a.unknownVolume());
            windows.add(row);
        }
        view.put("windows", windows);
        return view;
    }

    @PostMapping("/replay")
    public ReplayService.ReplayReport replay() {
        return replayService.replay();
    }
}
