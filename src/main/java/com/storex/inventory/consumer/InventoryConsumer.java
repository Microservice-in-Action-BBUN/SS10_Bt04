package com.storex.inventory.consumer;

import com.storex.inventory.model.OrderEvent;
import com.storex.inventory.service.InventoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * InventoryConsumer - Lắng nghe sự kiện tạo đơn hàng từ topic "order-events":
 * - Tự động khấu trừ số lượng sản phẩm trong kho.
 * - Nhờ DefaultErrorHandler và DeadLetterPublishingRecoverer, nếu bản tin bị lỗi hoặc hỏng định dạng JSON,
 *   nó sẽ được retry tối đa 3 lần và chuyển sang topic DLT ("order-events.DLT").
 * - Consumer sẽ tiếp tục commit offset và chuyển sang bản tin tiếp theo, không bao giờ bị "kẹt".
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class InventoryConsumer {

    private final InventoryService inventoryService;

    @KafkaListener(topics = "order-events", groupId = "inventory-group")
    public void consume(OrderEvent event) {
        log.info("Consumer tiếp nhận OrderEvent: orderId={}, productId={}, quantity={}",
                event.getOrderId(), event.getProductId(), event.getQuantity());

        inventoryService.deductStock(event.getProductId(), event.getQuantity());

        log.info("Đã xử lý khấu trừ kho thành công cho orderId={}", event.getOrderId());
    }
}
