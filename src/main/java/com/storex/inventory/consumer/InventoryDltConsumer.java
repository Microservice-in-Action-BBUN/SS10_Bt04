package com.storex.inventory.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Consumer chuyên biệt giám sát và tiếp nhận các bản tin lỗi từ Dead Letter Topic ("order-events.DLT"):
 * - Ghi log chi tiết lỗi, payload hỏng và vị trí offset ban đầu để phục vụ việc debug và can thiệp thủ công.
 */
@Component
@Slf4j
public class InventoryDltConsumer {

    @KafkaListener(topics = "order-events.DLT", groupId = "inventory-dlt-group")
    public void consumeDlt(
            ConsumerRecord<String, Object> record,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false) byte[] originalOffset,
            @Header(value = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage) {

        log.error("🚨 [DLT MONITOR] Phát hiện bản tin lỗi được đưa vào Dead Letter Queue!");
        log.error("Chi tiết bản tin DLT: Topic={}, Partition={}, Offset={}, Key={}",
                record.topic(), record.partition(), record.offset(), record.key());
        log.error("Nội dung Payload: {}", record.value());
        log.error("Thông điệp lỗi Exception: {}", exceptionMessage);
    }
}
