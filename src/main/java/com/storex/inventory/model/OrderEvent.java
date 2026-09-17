package com.storex.inventory.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Event đại diện cho sự kiện đặt hàng mới được bắn vào topic "order-events".
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long orderId;
    private Long productId;
    private Integer quantity;
    private Long timestamp;
}
