package com.shopflow.order.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventHandler {
    private final OrderEventProducer orderEventProducer;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleOrderCreated(OrderCreatedInternalEvent event) {
        log.info("트랜잭션 커밋 완료, Kafka 이벤트 발행: orderNumber={}",
                event.payload().orderNumber());

        orderEventProducer.publishOrderCreated(event.payload());
    }
}
