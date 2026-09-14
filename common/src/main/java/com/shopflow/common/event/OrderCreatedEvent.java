package com.shopflow.common.event;

import java.util.List;

public record OrderCreatedEvent(
        Long orderId,
        String orderNumber,
        Long userId,
        List<OrderItemPayload> items
) {
    public record OrderItemPayload(
            Long productId,
            Integer quantity
    ) {
    }
}
