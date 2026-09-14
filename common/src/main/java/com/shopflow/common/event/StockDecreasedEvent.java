package com.shopflow.common.event;

public record StockDecreasedEvent(
        Long orderId,
        String orderNumber
) {
}
