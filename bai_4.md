# BÀI 4: Giám Sát Chi Phí & Phân Tích Latency Trên Langfuse

---

## 1. Giới Thiệu & Bối Cảnh Bài Toán

Trong hệ thống **Rikkei Intelligent Banking & Assistant Suite (RikkeiPay)**, phân hệ trợ lý ảo AI phục vụ hàng chục ngàn lượt giao dịch mỗi ngày. Để tối ưu hóa hiệu quả tài chính và đảm bảo trải nghiệm người dùng mượt mà (SLA thời gian phản hồi $< 2$ giây), Ban Giám Đốc RikkeiPay yêu cầu bộ phận công nghệ phải thiết lập báo cáo giám sát:
1. **Kiểm Soát Chi Phí Token (Cost Tracking):** Theo dõi lượng token tiêu thụ và chi phí trung bình trên từng lượt gọi AI.
2. **So Sánh Chi Phí Model:** Đánh giá sự chênh lệch chi phí giữa mô hình **Gemini-2.5-Flash** (Google Cloud) và **DeepSeek-V3** (Open Source/Self-Hosted).
3. **Phân Tích Điểm Nghẽn Độ Trễ (Latency & Bottleneck Analysis):** Xác định chính xác độ trễ của quy trình RAG bắt nguồn từ tầng truy vấn cơ sở dữ liệu vector (**Retrieval**) hay từ tầng sinh văn bản của mô hình ngôn ngữ lớn (**Generation**).

---

## 2. Cơ Chế Tính Toán Chi Phí & Giám Sát Token Của Langfuse

### 2.1. Cơ Chế Tự Động Đếm Token (Input, Output, Total)
Langfuse hỗ trợ hai phương thức chính để thu thập số lượng token:

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│                        CƠ CHẾ THU THẬP TOKEN USAGE TRÊN LANGFUSE                  │
├───────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│  [ Spring AI / LangChain4j ] ────► [ LLM Provider (OpenAI, Gemini, DeepSeek) ]    │
│                                                   │                               │
│                                                   ▼                               │
│  [ Native Usage Metadata ] ◄───── [ API Response: prompt_tokens, completion_tokens│
│          │                                                                        │
│          ▼ (Tự động đọc Usage)                                                    │
│  ┌─────────────────────────────────────────────────────────────┐                  │
│  │                    LANGFUSE INGESTION ENGINE                │                  │
│  │  - Input Tokens: 245                                        │                  │
│  │  - Output Tokens: 110                                       │                  │
│  │  - Total Tokens: 355                                        │                  │
│  └──────────────────────────────┬──────────────────────────────┘                  │
│                                 │ (Ánh xạ Bảng giá Custom Model)                  │
│                                 ▼                                                 │
│  ┌─────────────────────────────────────────────────────────────┐                  │
│  │                 CALCULATED COST: $0.00004915                │                  │
│  └─────────────────────────────────────────────────────────────┘                  │
└───────────────────────────────────────────────────────────────────────────────────┘
```

1. **Thu thập tự động qua API Metadata (Primary Method):**
   - Hầu hết các nhà cung cấp LLM (OpenAI, Google Gemini, Anthropic, OpenRouter) đều trả về trường `usage` trong JSON payload của phản hồi.
   - Spring AI / LangChain4j tự động trích xuất các trường:
     - `prompt_tokens` (hoặc `input_tokens`): Số lượng token đầu vào (System prompt + User prompt + Context RAG).
     - `completion_tokens` (hoặc `output_tokens`): Số lượng token sinh ra từ mô hình.
     - `total_tokens`: Tổng số token tiêu thụ cho request.
   - Langfuse SDK / OTLP Exporter đóng gói dữ liệu này vào `GenerationPayload` và đẩy lên server.
2. **Cơ chế Tokenizer Fallback (Heuristic & Byte-Pair Encoding):**
   - Đối với các mô hình Local (như Ollama offline) hoặc Custom Internal Gateway không trả về metadata `usage`, Langfuse Client sử dụng thuật toán Tokenizer nội bộ (như thư viện `tiktoken` hoặc heuristic tính toán theo tỷ lệ ký tự/từ) để ước tính số lượng token trước khi gửi telemetry.

---

### 2.2. Cơ Chế Ánh Xạ Bảng Giá & Thiết Lập Custom Model Prices Trên Langfuse

Langfuse quản lý chi phí dựa trên bảng định nghĩa giá (**Model Pricing Table**) với công thức tổng quát:

$$\text{Total Cost (USD)} = (\text{Input Tokens} \times P_{\text{input}}) + (\text{Output Tokens} \times P_{\text{output}}) + (\text{Reasoning Tokens} \times P_{\text{reasoning}}) + (\text{Cache Read Tokens} \times P_{\text{cache}})$$

Trong đó:
- $P_{\text{input}}$: Đơn giá trên mỗi token đầu vào.
- $P_{\text{output}}$: Đơn giá trên mỗi token đầu ra.

#### 📊 Bảng So Sánh Chi Phí: Gemini-2.5-Flash vs. DeepSeek-V3

| Chỉ Số Đánh Giá | Mô Hình Google Gemini-2.5-Flash | Mô Hình DeepSeek-V3 | Nhận Xét So Sánh |
| :--- | :--- | :--- | :--- |
| **Giá Input Token (/ 1M tokens)** | **$0.075** ($0.000000075 / token) | **$0.14** (Cache Miss)<br>**$0.014** (Cache Hit 90%) | Nếu tận dụng tốt **Prompt Caching**, DeepSeek-V3 rẻ hơn 5 lần ở đầu vào. |
| **Giá Output Token (/ 1M tokens)**| **$0.300** ($0.000000300 / token) | **$0.280** ($0.000000280 / token) | Chi phí Output của DeepSeek-V3 rẻ hơn ~7% so với Gemini-2.5-Flash. |
| **Độ trễ TTFT (Time to First Token)**| 🟢 **Rất Nhanh (~250 - 400ms)** | 🟡 **Khá (~400 - 650ms)** | Gemini-2.5-Flash phản hồi ban đầu nhanh hơn đáng kể. |
| **Chi phí ước tính cho 100,000 requests/ngày tại RikkeiPay**<br>*(Giả định: 1,500 Input Tokens + 300 Output Tokens/req)* | - Input: $100k \times 1.5k \times \$0.075 / 1M = \$11.25$<br>- Output: $100k \times 0.3k \times \$0.30 / 1M = \$9.00$<br>👉 **Tổng: \$20.25 / ngày (~$607.5 / tháng)** | - Input (Hit cache 70%): $\approx \$7.77$<br>- Output: $100k \times 0.3k \times \$0.28 / 1M = \$8.40$<br>👉 **Tổng: \$16.17 / ngày (~$485.1 / tháng)** | **DeepSeek-V3 tiết kiệm hơn ~20.1%** tổng chi phí vận hành hàng tháng. |

---

### 2.3. Hướng Dẫn Từng Bước Thiết Lập Bảng Giá Model Tùy Chỉnh Trên Langfuse UI

Để thiết lập bảng giá cho một mô hình mới (ví dụ: `deepseek-v3` hoặc model fine-tuned nội bộ của RikkeiPay):

1. **Bước 1:** Đăng nhập vào Langfuse Dashboard (`http://localhost:3000` hoặc host của ngân hàng).
2. **Bước 2:** Điều hướng tới menu **Project Settings** (biểu tượng bánh răng góc dưới bên trái) $\rightarrow$ chọn tab **Models**.
3. **Bước 3:** Nhấn nút **Add Model Definition** (+ Thêm mô hình mới).
4. **Bước 4:** Điền các thông số cấu hình:
   - **Model Name Matcher (Regex):** `(?i)^(deepseek-v3|deepseek/deepseek-chat).*$` *(giúp tự động khớp tên model bất kể chữ hoa/thường)*.
   - **Unit:** Chọn `TOKENS`.
   - **Input Price:** `0.00000014` (tương đương $0.14 / 1M tokens).
   - **Output Price:** `0.00000028` (tương đương $0.28 / 1M tokens).
   - **Start Date:** Chọn ngày bắt đầu áp dụng bảng giá (ví dụ: ngày hôm nay).
5. **Bước 5:** Nhấn **Save**. Ngay lập tức, toàn bộ các Trace phát sinh từ thời điểm cấu hình sẽ được tự động nhân đơn giá và hiển thị số tiền USD chính xác trên Dashboard.

---

## 3. Hướng Dẫn Phân Tích Biểu Đồ Latency Để Xác Định Điểm Nghẽn (Bottleneck) Quy Trình RAG

### 3.1. Cấu Trúc Trace Tree Chuẩn Của Một Truy Vấn RAG

Một chu trình RAG hoàn chỉnh được phân rã thành biểu đồ dạng thác nước (Waterfall Span Tree):

```
[TRACE] BankingRAGQuery ────────────────────────────────────────── Total: 2,450 ms
   │
   ├── [SPAN] EmbeddingGeneration ────────────────────────────── 85 ms (3.5%)
   │      └── Model: nomic-embed-text (Local/Ollama)
   │
   ├── [SPAN] VectorDBSearch (Retrieval Step) ────────────────── 165 ms (6.7%)
   │      └── Query: pgvector HNSW Cosine (Top-k=4, filter: account_type='VIP')
   │
   ├── [SPAN] ContextAugmentation / Re-ranking ────────────────── 30 ms (1.2%)
   │      └── Ghép context vào System Prompt
   │
   └── [GENERATION] LLMSynthesis (Generation Step) ────────────── 2,170 ms (88.6%) ⚠️ BOTTLENECK
          ├── Time to First Token (TTFT): 380 ms
          └── Token Streaming Generation: 1,790 ms (350 tokens generated)
```

---

### 3.2. Phương Pháp Chẩn Đoán & Định Vị Bottleneck

| Tình Huống | Biểu Hiện Trên Biểu Đồ Langfuse | Nguyên Nhân Kỹ Thuật Cốt Lõi | Giải Pháp Khắc Phục Tối Ưu |
| :--- | :--- | :--- | :--- |
| **Trường Hợp 1: Bottleneck Tại Khâu RETRIEVAL (Vector DB)** | - Span `VectorDBSearch` chiếm **$> 40\%$** tổng thời gian xử lý (ví dụ: mất $> 1,000$ms trong khi LLM chỉ mất 500ms).<br>- Biểu đồ p95 / p99 Latency của DB tăng vọt khi nhiều người truy vấn đồng thời. | 1. Bảng `vector_store` chưa đánh chỉ mục **HNSW** (đang quét tuần tự Sequential Scan).<br>2. Vector Dimension quá lớn mà không dùng nén vector (Product Quantization).<br>3. `maximum-pool-size` của HikariCP quá nhỏ gây nghẽn chờ kết nối DB.<br>4. Không đánh B-Tree index trên các cột lọc metadata JSONB (`user_id`, `category`). | 1. Chạy DDL tạo chỉ mục HNSW: `CREATE INDEX ON vector_store USING hnsw (embedding vector_cosine_ops);`.<br>2. Tăng connection pool hoặc bổ sung Connection Pooler (PgBouncer/Supavisor).<br>3. Đánh index GIN/B-Tree trên các trường metadata thường xuyên filter. |
| **Trường Hợp 2: Bottleneck Tại Khâu GENERATION (LLM)** | - Step `LLMSynthesis` chiếm **$> 75 - 90\%$** tổng thời gian.<br>- Chỉ số **TTFT (Time To First Token)** $> 2,000$ms.<br>- Tốc độ sinh token (Tokens per second) quá thấp ($< 15$ tokens/s). | 1. Prompt nạp Context RAG quá dài ($> 8,000$ tokens) khiến LLM mất nhiều thời gian xử lý Prefill.<br>2. Không kích hoạt cơ chế **Streaming Response** (`Flux<String>` / SSE) khiến người dùng phải đợi sinh toàn bộ text.<br>3. Sử dụng mô hình quá cồng kềnh (ví dụ: chạy full model 70B thay vì Flash/Distilled model 8B/Flash). | 1. Cắt giảm số lượng chunk RAG (giảm Top-k từ 10 xuống 3-4 chunks chất lượng cao).<br>2. Bật tính năng **Streaming Response** để giảm perceived latency cho khách hàng.<br>3. Chuyển đổi sang các mô hình tối ưu tốc độ như **Gemini-2.5-Flash** hoặc **DeepSeek-V3**. |

---

### 3.3. Đọc Biểu Đồ Phân Vị (Percentiles p50, p90, p95, p99) Trên Dashboard
- **p50 (Median Latency):** Thời gian phản hồi trung vị của 50% người dùng. Nếu p50 $< 1.2$s là hệ thống đạt chuẩn mượt mà.
- **p95 / p99 (Long-Tail Latency):** Phản ánh trải nghiệm của 1% - 5% khách hàng gặp sự cố chậm nhất. Nếu p99 vọt lên $> 10$s, đó là dấu hiệu của việc nghẽn Connection Pool DB, Cold-start mô hình hoặc nghẽn hàng đợi (Queue saturation) tại AI Gateway.

---

## 4. Mã Nguồn Java Minh Họa Gửi Token Usage & Latency Thủ Công

Dưới đây là lớp `CustomModelObservabilityService.java` hoàn chỉnh minh họa cách tự tính toán token và gửi telemetry thủ công sang Langfuse đối với các mô hình On-Premise/Custom không có metadata:

*Đường dẫn file: [src/main/java/com/rikkeipay/service/CustomModelObservabilityService.java](file:///d:/%5BIT-213%5D%20AI%20Integration%20in%20Action/Ss10/4/src/main/java/com/rikkeipay/service/CustomModelObservabilityService.java)*

```java
package com.rikkeipay.service;

import io.langfuse.client.LangfuseClient;
import io.langfuse.client.model.Trace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
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
    // Ví dụ: DeepSeek-V3 Custom Hosting
    private static final double INPUT_TOKEN_RATE = 0.00000014;   // $0.14 / 1M input tokens
    private static final double OUTPUT_TOKEN_RATE = 0.00000028;  // $0.28 / 1M output tokens

    public CustomModelObservabilityService(LangfuseClient langfuseClient) {
        this.langfuseClient = langfuseClient;
    }

    /**
     * Xử lý truy vấn RAG hoàn chỉnh kết hợp tính toán và gửi Token Usage & Latency thủ công.
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
        // BƯỚC 2: GENERATION STEP (Gọi Custom LLM & Đo độ trễ)
        // -----------------------------------------------------------------------------------------
        String systemPrompt = "Bạn là trợ lý RikkeiPay. Hãy dựa vào tài liệu sau để trả lời: " + retrievedContext;
        String fullInputPrompt = systemPrompt + "\nCâu hỏi: " + query;

        long generationStartMs = System.currentTimeMillis();
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
     * Thuật toán ước lượng số token chuẩn xác cho tiếng Việt và tiếng Anh.
     */
    private int estimateTokenCount(String text) {
        if (text == null || text.isBlank()) return 0;
        String clean = text.trim();
        int charBased = (int) Math.ceil(clean.length() / 3.5);
        int wordBased = clean.split("\\s+").length * 2;
        return Math.max(1, (charBased + wordBased) / 2);
    }

    private String mockVectorDatabaseSearch(String query) {
        try {
            Thread.sleep(120); // Giả lập độ trễ pgvector HNSW search
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
```

---

## 5. Kết Luận & Khuyến Nghị Vận Hành
1. **Kiểm Soát Chi Phí:** Áp dụng **DeepSeek-V3** kết hợp kỹ thuật **Prompt Caching** giúp giảm tới **$20.1\%$** chi phí token hàng tháng so với Gemini-2.5-Flash đối với các prompt có context RAG lặp lại nhiều lần.
2. **Tối Ưu Trải Nghiệm Khách Hàng:** Bật **Streaming Response** và đánh chỉ mục **HNSW** trên PostgreSQL pgvector giúp kiểm soát chỉ số **p95 Latency $< 1.5$s**, đảm bảo SLA khắt khe của ngân hàng số RikkeiPay.
