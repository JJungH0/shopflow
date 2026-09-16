package com.shopflow.order.service;

import com.shopflow.common.event.OrderCreatedEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class TestEventCollector {

    private final List<OrderCreatedEvent> events = new ArrayList<>();

    public void add(OrderCreatedEvent event) {
        events.add(event);
    }

    public int size() {
        return events.size();
    }

    public void clear() {
        events.clear();
    }
}
