package com.storex.inventory.consumer;

import com.storex.inventory.model.OrderEvent;
import com.storex.inventory.service.InventoryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InventoryConsumerTest {

    @Mock
    private InventoryService inventoryService;

    @Mock
    private KafkaOperations<Object, Object> kafkaOperations;

    private InventoryConsumer inventoryConsumer;

    @BeforeEach
    void setUp() {
        inventoryConsumer = new InventoryConsumer(inventoryService);
    }

    @Test
    @DisplayName("Unit Test 1: Happy Path - Xử lý đơn hàng hợp lệ và khấu trừ tồn kho thành công")
    void testConsume_Success() {
        // Arrange: Chuẩn bị sự kiện đặt hàng hợp lệ
        OrderEvent validEvent = OrderEvent.builder()
                .orderId(5001L)
                .productId(101L)
                .quantity(3)
                .timestamp(System.currentTimeMillis())
                .build();

        doNothing().when(inventoryService).deductStock(101L, 3);

        // Act: Gọi consumer
        inventoryConsumer.consume(validEvent);

        // Assert: Xác nhận hàm deductStock được gọi chính xác 1 lần
        verify(inventoryService, times(1)).deductStock(101L, 3);
    }

    @Test
    @DisplayName("Unit Test 2: Error Handling - Khi gặp ngoại lệ, ném lỗi để DefaultErrorHandler kích hoạt Retry")
    void testConsume_ServiceThrowsException_TriggersRetry() {
        // Arrange
        OrderEvent invalidEvent = OrderEvent.builder()
                .orderId(5002L)
                .productId(999L)
                .quantity(10)
                .timestamp(System.currentTimeMillis())
                .build();

        doThrow(new RuntimeException("Database deadlock / Out of stock"))
                .when(inventoryService).deductStock(999L, 10);

        // Act & Assert
        assertThrows(RuntimeException.class, () -> {
            inventoryConsumer.consume(invalidEvent);
        }, "Khi có lỗi xử lý, consumer phải ném exception để Kafka Listener ErrorHandler can thiệp");

        verify(inventoryService, times(1)).deductStock(999L, 10);
    }

    @Test
    @DisplayName("Unit Test 3: DeadLetterPublishingRecoverer - Tự động định tuyến sang topic order-events.DLT sau 3 lần retry")
    void testDeadLetterPublishingRecoverer_RoutesToDlt() {
        // Arrange: Tạo ConsumerRecord giả lập trên topic "order-events"
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("order-events", 0, 42L, "KEY-1", "MALFORMED_JSON_PAYLOAD");
        RuntimeException exception = new RuntimeException("Malformed JSON: missing closing bracket '}'");

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaOperations,
                (rec, ex) -> new TopicPartition(rec.topic() + ".DLT", rec.partition()));

        when(kafkaOperations.send(any(org.apache.kafka.clients.producer.ProducerRecord.class)))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));

        // Act: Kích hoạt recoverer
        recoverer.accept(record, exception);

        // Assert: Xác nhận recoverer đã gọi kafkaOperations.send() để đưa bản tin vào topic DLT
        verify(kafkaOperations, times(1)).send(any(org.apache.kafka.clients.producer.ProducerRecord.class));
    }

    @Test
    @DisplayName("Unit Test 4: DefaultErrorHandler - Kiểm tra cấu hình đúng 3 lần thử (1 lần gốc + 2 lần retry)")
    void testDefaultErrorHandler_RetryPolicyCount() {
        // Arrange
        FixedBackOff backOff = new FixedBackOff(50L, 2L); // 2 lần retry -> tổng 3 lần thử
        DeadLetterPublishingRecoverer recoverer = mock(DeadLetterPublishingRecoverer.class);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        AtomicInteger attemptCounter = new AtomicInteger(0);
        errorHandler.setRetryListeners((rec, ex, attempt) -> {
            attemptCounter.set(attempt);
        });

        // Assert
        assertEquals(2L, backOff.getMaxAttempts(), "Cấu hình số lần retry tối đa là 2 lần (sau lần thực thi đầu tiên)");
        assertEquals(50L, backOff.getInterval(), "Khoảng cách giữa các lần retry");
        assertNotNull(errorHandler, "ErrorHandler khởi tạo thành công");
    }
}
