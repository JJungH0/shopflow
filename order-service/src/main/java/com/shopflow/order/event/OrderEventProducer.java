package com.shopflow.order.event;

import com.shopflow.common.event.KafkaTopic;
import com.shopflow.common.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishOrderCreated(OrderCreatedEvent event) {
        kafkaTemplate.send(
                KafkaTopic.ORDER_CREATED,
                String.valueOf(event.orderId()),
                event
        ).whenComplete((result, ex) -> {
            if (Objects.nonNull(ex)) {
                log.error("주문 생성 이벤트 발행 실패: orderNumber={}", event.orderNumber());
            } else {
                log.info("주문 생성 이벤트 발행: orderNumber={}, partition={}",
                        event.orderNumber(),
                        result.getRecordMetadata().partition());
            }
        });
    }
}
