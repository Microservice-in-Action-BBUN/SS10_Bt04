package com.storex.inventory.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Cấu hình xử lý lỗi chuyên sâu cho Kafka Consumer:
 * 1. DefaultErrorHandler với FixedBackOff(1000L, 2L) -> Tổng cộng 3 lần thử (1 lần gốc + 2 lần retry).
 * 2. DeadLetterPublishingRecoverer: Khi hết 3 lần thử vẫn thất bại, tự động đẩy bản tin sang topic Dead Letter Queue ("order-events.DLT").
 * 3. Cam kết commit offset an toàn để Consumer tiếp tục xử lý các bản tin kế tiếp mà không bao giờ bị "kẹt".
 */
@Configuration
@Slf4j
public class KafkaConsumerConfig {

    public static final String ORDER_EVENTS_TOPIC = "order-events";
    public static final String DLT_TOPIC = "order-events.DLT";

    /**
     * Khởi tạo DeadLetterPublishingRecoverer để xuất bản tin nhắn lỗi sang topic DLT.
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaOperations<Object, Object> kafkaOperations) {
        return new DeadLetterPublishingRecoverer(kafkaOperations,
                (record, exception) -> {
                    log.error("DeadLetterPublishingRecoverer: Bản tin tại topic [{}], partition [{}], offset [{}] " +
                                    "đã cạn 3 lần retry. Đang chuyển tiếp vào Dead Letter Topic [{}]! Nguyên nhân: {}",
                            record.topic(), record.partition(), record.offset(), DLT_TOPIC, exception.getMessage());
                    return new TopicPartition(DLT_TOPIC, record.partition());
                });
    }

    /**
     * Cấu hình DefaultErrorHandler kiểm soát Retry 3 lần và kích hoạt DLT.
     */
    @Bean
    public DefaultErrorHandler defaultErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        // Cấu hình: interval = 1000ms (1 giây), maxAttempts = 2 (tổng cộng 1 lần ban đầu + 2 lần retry = 3 lần thử)
        FixedBackOff backOff = new FixedBackOff(1000L, 2L);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Đăng ký Retry Listener ghi log chi tiết từng lần retry
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.warn("Kafka Retry Listener: Đang thử lại lần {}/3 cho bản tin tại offset [{}], key [{}]. Lỗi: {}",
                    deliveryAttempt, record.offset(), record.key(), ex.getMessage());
        });

        return errorHandler;
    }

    /**
     * Cấu hình Kafka Listener Container Factory tích hợp DefaultErrorHandler.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            DefaultErrorHandler defaultErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(defaultErrorHandler);

        return factory;
    }
}
