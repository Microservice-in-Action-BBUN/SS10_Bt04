package com.storex.inventory.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Service xử lý nghiệp vụ quản lý tồn kho tại StoreX.
 */
@Service
@Slf4j
public class InventoryService {

    // Giả lập kho hàng trong bộ nhớ
    private final ConcurrentHashMap<Long, Integer> stockTable = new ConcurrentHashMap<>();

    public InventoryService() {
        // Khởi tạo tồn kho ban đầu
        stockTable.put(101L, 500);
        stockTable.put(102L, 200);
        stockTable.put(103L, 50);
    }

    /**
     * Khấu trừ số lượng sản phẩm trong kho khi nhận đơn hàng mới.
     *
     * @param productId ID sản phẩm
     * @param quantity  Số lượng cần trừ
     */
    public void deductStock(Long productId, Integer quantity) {
        if (productId == null || quantity == null || quantity <= 0) {
            throw new IllegalArgumentException("Thông tin sản phẩm hoặc số lượng trừ kho không hợp lệ: productId="
                    + productId + ", quantity=" + quantity);
        }

        log.info("Bắt đầu trừ tồn kho cho sản phẩm ID {}: Số lượng = {}", productId, quantity);
        stockTable.compute(productId, (id, currentStock) -> {
            int current = (currentStock != null) ? currentStock : 100;
            if (current < quantity) {
                log.warn("Cảnh báo: Sản phẩm ID {} không đủ tồn kho (Còn {}, yêu cầu {}).", id, current, quantity);
            }
            int updated = Math.max(0, current - quantity);
            log.info("Khấu trừ thành công. Tồn kho mới cho sản phẩm ID {}: {}", id, updated);
            return updated;
        });
    }

    public int getStock(Long productId) {
        return stockTable.getOrDefault(productId, 0);
    }
}
