package com.shopflow.common.event;

public record StockFailedEvent(
        Long orderId,
        String orderNumber,
        Long productId,
        String reason
) {
}
