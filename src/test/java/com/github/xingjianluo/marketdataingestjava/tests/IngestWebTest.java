package com.github.xingjianluo.marketdataingestjava.tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Web 层冒烟：通过 HTTP 提交一条合法与一条重复事件，
 * 验证 200/422 与结构化拒绝原因可被调用方区分。
 * 使用每次运行独立的临时 stateDir，避免跨运行残留状态影响位点判定。
 */
@SpringBootTest
@AutoConfigureMockMvc
class IngestWebTest {

    @TempDir
    static Path tempStateDir;

    @DynamicPropertySource
    static void stateDir(DynamicPropertyRegistry registry) {
        registry.add("market.ingest.state-dir", () -> tempStateDir.toString());
    }

    @Autowired
    private MockMvc mockMvc;

    private String envelope(long seq, String eventId) {
        return "{\"source\":\"WEB\",\"seq\":" + seq + ",\"eventId\":\"" + eventId
                + "\",\"schemaVersion\":\"v2\",\"payload\":{"
                + "\"symbol\":\"BTCUSDT\",\"eventTime\":1000,\"price\":100,\"size\":1,\"side\":\"BUY\"}}";
    }

    @Test
    void httpIngestAcceptsThenRejectsDuplicateAndAggregationsVisible() throws Exception {
        mockMvc.perform(post("/api/ingest").contentType("application/json").content(envelope(0, "WEB-0")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true));

        // 第二条复用同 eventId、位点连续推进到 1：判定 DUPLICATE 且 422
        mockMvc.perform(post("/api/ingest").contentType("application/json").content(envelope(1, "WEB-0")))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.accepted").value(false))
                .andExpect(jsonPath("$.reason").value("DUPLICATE"))
                .andExpect(jsonPath("$.detail").exists());

        mockMvc.perform(get("/api/aggregations"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.watermark").value(1000))
                .andExpect(jsonPath("$.windows[0].symbol").value("BTCUSDT"));
    }
}
