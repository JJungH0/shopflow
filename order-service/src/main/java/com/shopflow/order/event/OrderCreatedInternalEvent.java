package com.shopflow.order.event;

import com.shopflow.common.event.OrderCreatedEvent;
import com.shopflow.order.dto.OrderCreateRequest;

public record OrderCreatedInternalEvent(
        OrderCreatedEvent payload
) {
}
