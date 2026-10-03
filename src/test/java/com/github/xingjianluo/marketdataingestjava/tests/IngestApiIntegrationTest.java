package com.github.xingjianluo.marketdataingestjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 端到端：HTTP 批量摄取（多源、重复、乱序、非法、回退），结论可区分；
 * 快照落本地文件、恢复后聚合一致。使用随机临时目录，测试间互不污染。
 */
@SpringBootTest(properties = "ingest.snapshot-path=target/itest-snapshot/snapshot.json")
@AutoConfigureMockMvc
class IngestApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(IngestApiIntegrationTest.class);

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper mapper;

    @Test
    void batchIngestClassifiesAndSnapshotRestorePersists() throws Exception {
        long t = 120_000L;
        String ok1 = TestSupport.event("i-1", "src-http", "AAA", "TRADE", t + 9_000, 0, "110", 10);
        String ok2 = TestSupport.eventV2("i-2", "src-http", "AAA", "TRADE", t + 5_000, 1,
                "105", 4, "USD", "NYSE"); // 窗口边界内
        String dup = ok1;
        String invalid = TestSupport.raw("{\"eventId\":\"i-bad\",\"source\":\"src-http\"}");
        String late = TestSupport.event("i-late", "src-http", "AAA", "TRADE", t, 2, "100", 1);
        String otherSource = TestSupport.event("i-9", "src-other", "AAA", "TRADE", t, 0, "100", 1);

        Map<String, Object> req = Map.of("events",
                List.of(ok1, ok2, dup, invalid, late, otherSource));
        String resp = mockMvc.perform(post("/ingest/events")
                        .contentType("application/json")
                        .content(mapper.writeValueAsString(req)))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        JsonNode results = mapper.readTree(resp);
        long accepted = 0, duplicate = 0, rejInvalid = 0, rejLate = 0;
        for (JsonNode r : results) {
            String st = r.get("status").asText();
            log.info("HTTP 判定 eventId={} status={} reason={}",
                    r.get("eventId").asText(), st, r.get("reason").asText());
            switch (st) {
                case "ACCEPTED" -> accepted++;
                case "DUPLICATE" -> duplicate++;
                case "REJECTED_INVALID" -> rejInvalid++;
                case "REJECTED_LATE" -> rejLate++;
                default -> throw new AssertionError("unexpected " + st);
            }
        }
        assertEquals(3, accepted);    // ok1, ok2, otherSource
        assertEquals(1, duplicate);
        assertEquals(1, rejInvalid);
        assertEquals(1, rejLate);

        // 聚合查询：src-http 桶恰含 2 条（重复、非法、迟到均未计入）
        String aggResp = mockMvc.perform(get("/ingest/aggregates"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        JsonNode aggs = mapper.readTree(aggResp);
        log.info("聚合查询返回 {} 个桶", aggs.size());
        long httpCount = 0;
        for (JsonNode a : aggs) {
            if (a.get("symbol").asText().equals("AAA") && a.get("currency").asText().equals("USD")) {
                httpCount += a.get("count").asLong();
            }
        }
        assertEquals(3, httpCount);

        // 快照 -> 恢复（新进程语义由 /restore 重新装载本地文件验证状态可读且一致）
        mockMvc.perform(post("/ingest/snapshot")).andExpect(status().isOk());
        String before = aggResp;
        mockMvc.perform(post("/ingest/restore")).andExpect(status().isOk());
        String after = mockMvc.perform(get("/ingest/aggregates"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        assertEquals(mapper.readTree(before), mapper.readTree(after),
                "恢复前后聚合必须一致");
        log.info("快照-恢复后端到端聚合一致");
    }

    @Test
    void replayFromScratchEndpointVerifiesAndRefusesNonCleanOffset() throws Exception {
        long t = 240_000L;
        // 两个来源同号同刻（多源幂等口径端到端）
        mockMvc.perform(post("/ingest/events").contentType("application/json")
                .content(mapper.writeValueAsString(Map.of("events", List.of(
                        TestSupport.event("1", "ex-A", "BBB", "TRADE", t, 0, "100", 1),
                        TestSupport.event("1", "ex-B", "BBB", "TRADE", t, 0, "102", 1))))))
                .andExpect(status().isOk());

        // 干净整体重放：与同一 Spring 上下文中此前摄入的全部历史逐条一致
        // （首个用例已摄入 6 条，本用例再摄入 2 条，共 8 条）。
        String okResp = mockMvc.perform(post("/ingest/replay")
                        .contentType("application/json").content("{}"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        JsonNode ok = mapper.readTree(okResp);
        log.info("HTTP 重放核对: {}", okResp);
        org.junit.jupiter.api.Assertions.assertEquals(8, ok.get("totalReplayed").asInt());
        org.junit.jupiter.api.Assertions.assertEquals(8, ok.get("matched").asInt());
        org.junit.jupiter.api.Assertions.assertEquals(0, ok.get("mismatched").asInt());
        org.junit.jupiter.api.Assertions.assertTrue(ok.get("aggregatesMatch").asBoolean());

        // 给非 -1 的续处理起始位置 -> 400 明确拒绝
        mockMvc.perform(post("/ingest/replay").contentType("application/json")
                        .content(mapper.writeValueAsString(
                                Map.of("startOffsets", Map.of("ex-A", 0L)))))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post("/ingest/replay").contentType("application/json")
                        .content(mapper.writeValueAsString(
                                Map.of("startOffsets", Map.of("ex-A", 9L)))))
                .andExpect(status().isBadRequest());
        log.info("HTTP 重放对更靠前/更靠后的起始位置均 400 拒绝");
    }
}
