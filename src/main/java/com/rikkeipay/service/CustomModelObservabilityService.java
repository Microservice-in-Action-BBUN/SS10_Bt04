package com.rikkeipay.service;

import io.langfuse.client.LangfuseClient;
import io.langfuse.client.model.Trace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Service minh họa gửi Token Usage và Latency Metrics thủ công sang Langfuse
 * đối với các Custom Model / Private LLM On-Premise không tự động trả về trường usage metadata.
 */
@Service
public class CustomModelObservabilityService {

    private static final Logger log = LoggerFactory.getLogger(CustomModelObservabilityService.class);

    private final LangfuseClient langfuseClient;

    // Bảng giá tùy chỉnh áp dụng cho Custom Private Model (Đơn vị: USD / Token)
    // Ví dụ: DeepSeek-V3 On-Premise / Custom Hosting
    private static final double INPUT_TOKEN_RATE = 0.00000014;   // $0.14 / 1M input tokens
    private static final double OUTPUT_TOKEN_RATE = 0.00000028;  // $0.28 / 1M output tokens

    public CustomModelObservabilityService(LangfuseClient langfuseClient) {
        this.langfuseClient = langfuseClient;
    }

    /**
     * Xử lý truy vấn RAG hoàn chỉnh kết hợp tính toán và gửi Token Usage & Latency thủ công.
     *
     * @param userId mã người dùng
     * @param sessionId mã phiên
     * @param query câu hỏi truy vấn tài chính
     * @return kết quả trả lời của AI
     */
    public String executeCustomRagWorkflow(String userId, String sessionId, String query) {
        String traceId = UUID.randomUUID().toString();
        long workflowStartMs = System.currentTimeMillis();

        log.info("[CustomModelObservabilityService] Bắt đầu Trace RAG workflow ID: [{}]", traceId);

        // 1. Khởi tạo Trace chính
        Trace trace = langfuseClient.trace(new Trace()
                .id(traceId)
                .name("RikkeiPay-Custom-RAG")
                .userId(userId)
                .sessionId(sessionId)
                .input(Map.of("user_query", query))
        );

        // -----------------------------------------------------------------------------------------
        // BƯỚC 1: RETRIEVAL STEP (Truy vấn cơ sở dữ liệu Vector)
        // -----------------------------------------------------------------------------------------
        long retrievalStartMs = System.currentTimeMillis();
        String retrievedContext = mockVectorDatabaseSearch(query);
        long retrievalDurationMs = System.currentTimeMillis() - retrievalStartMs;

        // -----------------------------------------------------------------------------------------
        // BƯỚC 2: GENERATION STEP (Gọi Custom LLM & Tính Token thủ công)
        // -----------------------------------------------------------------------------------------
        String systemPrompt = "Bạn là trợ lý RikkeiPay. Hãy dựa vào tài liệu sau để trả lời: " + retrievedContext;
        String fullInputPrompt = systemPrompt + "\nCâu hỏi: " + query;

        long generationStartMs = System.currentTimeMillis();
        // Giả lập gọi Custom Model / Local LLM On-Premise
        String llmOutput = callCustomOnPremiseModel(fullInputPrompt);
        long generationDurationMs = System.currentTimeMillis() - generationStartMs;

        // -----------------------------------------------------------------------------------------
        // BƯỚC 3: TÍNH TOÁN TOKEN USAGE & CHI PHÍ THỦ CÔNG (Manual Token Counting & Cost Estimation)
        // -----------------------------------------------------------------------------------------
        int promptTokens = estimateTokenCount(fullInputPrompt);
        int completionTokens = estimateTokenCount(llmOutput);
        int totalTokens = promptTokens + completionTokens;

        double estimatedCostUsd = (promptTokens * INPUT_TOKEN_RATE) + (completionTokens * OUTPUT_TOKEN_RATE);

        long totalWorkflowDurationMs = System.currentTimeMillis() - workflowStartMs;

        log.info("[CustomModelObservabilityService] Token Usage: Prompt={}, Output={}, Total={}, Cost=${}",
                promptTokens, completionTokens, totalTokens, String.format("%.6f", estimatedCostUsd));
        log.info("[CustomModelObservabilityService] Latency Breakdown: Retrieval={}ms, Generation={}ms, Total={}ms",
                retrievalDurationMs, generationDurationMs, totalWorkflowDurationMs);

        // -----------------------------------------------------------------------------------------
        // BƯỚC 4: GỬI TELEMETRY METRICS VỀ LANGFUSE
        // -----------------------------------------------------------------------------------------
        trace.output(Map.of(
                "final_answer", llmOutput,
                "token_usage", Map.of(
                        "prompt_tokens", promptTokens,
                        "completion_tokens", completionTokens,
                        "total_tokens", totalTokens,
                        "estimated_cost_usd", estimatedCostUsd
                ),
                "latency_metrics", Map.of(
                        "retrieval_latency_ms", retrievalDurationMs,
                        "generation_latency_ms", generationDurationMs,
                        "total_latency_ms", totalWorkflowDurationMs,
                        "bottleneck_component", retrievalDurationMs > generationDurationMs ? "VECTOR_DATABASE" : "LLM_GENERATION"
                ),
                "model_name", "deepseek-v3-onpremise-custom"
        ));

        return llmOutput;
    }

    /**
     * Thuật toán ước lượng số token chuẩn xác cho tiếng Việt và tiếng Anh khi model không hỗ trợ native tokenizer.
     * Heuristic: Tiếng Việt trung bình ~3.2 ký tự/token do các từ ghép và dấu thanh UTF-8.
     */
    private int estimateTokenCount(String text) {
        if (text == null || text.isBlank()) {
            return 0;
        }
        // Chuẩn hóa khoảng trắng
        String clean = text.trim();
        // Ước lượng dựa trên độ dài ký tự và số từ
        int charBased = (int) Math.ceil(clean.length() / 3.5);
        int wordBased = clean.split("\\s+").length * 2;
        return Math.max(1, (charBased + wordBased) / 2);
    }

    private String mockVectorDatabaseSearch(String query) {
        try {
            Thread.sleep(120); // Giả lập độ trễ truy vấn pgvector HNSW
        } catch (InterruptedException ignored) {}
        return "Tài liệu ngân hàng: Hạn mức chuyển tiền nhanh NAPAS 247 qua RikkeiPay là 500,000,000 VND/giao dịch.";
    }

    private String callCustomOnPremiseModel(String prompt) {
        try {
            Thread.sleep(850); // Giả lập độ trễ sinh văn bản của LLM
        } catch (InterruptedException ignored) {}
        return "Theo quy định của RikkeiPay, hạn mức chuyển tiền nhanh NAPAS 24/7 tối đa là 500,000,000 VND cho mỗi giao dịch.";
    }
}
