package com.shopflow.order.service;

import com.shopflow.common.event.OrderCreatedEvent;
import com.shopflow.order.event.OrderEventProducer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;

@TestConfiguration
public class TestKafkaConfig {

    @Bean
    @Primary
    public OrderEventProducer testOrderEventProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            TestEventCollector collector
    ) {
        return new OrderEventProducer(kafkaTemplate) {
            @Override
            public void publishOrderCreated(OrderCreatedEvent event) {
                collector.add(event);
            }
        };
    }
}
