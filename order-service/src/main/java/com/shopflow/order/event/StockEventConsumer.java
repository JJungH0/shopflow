package com.shopflow.order.event;

import com.shopflow.common.event.KafkaTopic;
import com.shopflow.common.event.StockDecreasedEvent;
import com.shopflow.common.event.StockFailedEvent;
import com.shopflow.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class StockEventConsumer {

    private final OrderService orderService;

    @KafkaListener(
            topics = KafkaTopic.STOCK_DECREASED,
            groupId = "order-service"
    )
    public void handleStockDecreased(StockDecreasedEvent event) {
        log.info("재고 차감 완료 이벤트 수신: orderNumber={}", event.orderNumber());

        orderService.markAsPaid(event.orderId());
    }

    @KafkaListener(
            topics = KafkaTopic.STOCK_FAILED,
            groupId = "order-service"
    )
    public void handleStockFailed(StockFailedEvent event) {
        log.info("재고 차감 실패 이벤트 수신: orderNumber={}", event.orderNumber());

        orderService.cancelOrder(event.orderId(), event.reason());
    }
}
