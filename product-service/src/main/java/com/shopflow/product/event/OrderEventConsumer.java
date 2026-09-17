package com.shopflow.product.event;

import com.shopflow.common.event.KafkaTopic;
import com.shopflow.common.event.OrderCreatedEvent;
import com.shopflow.common.event.StockDecreasedEvent;
import com.shopflow.common.event.StockFailedEvent;
import com.shopflow.product.service.RedissonLockStockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Slf4j @Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final RedissonLockStockService stockService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

//    @KafkaListener(
//            topics = KafkaTopic.ORDER_CREATED,
//            groupId = "product-service"
//    )
//    public void handleOrderCreated(OrderCreatedEvent event) {
//        log.info("주문 생성 이벤트 수신: orderNumber={}", event.orderNumber());
//
//        try {
//            event.items().forEach(
//                    item -> stockService.decreaseStock(item.productId(), item.quantity())
//            );
//
//            publishStockDecreased(event);
//        } catch (Exception e) {
//            log.error("재고 차감 실패: orderNumber={}, reason={}", event.orderNumber(), e.getMessage());
//
//            publishStockFailed(event, e.getMessage());
//        }
//    }

    @KafkaListener(
            topics = KafkaTopic.ORDER_CREATED,
            groupId = "product-service")
    public void handleOrderCreated(String message) {
        OrderCreatedEvent event = objectMapper.readValue(message, OrderCreatedEvent.class);

        log.info("주문 생성 이벤트 수신: orderNumber={}", event.orderNumber());

        try {
            event.items().forEach(item ->
                    stockService.decreaseStock(item.productId(), item.quantity()));

            publishStockDecreased(event);
        } catch (Exception e) {
            log.error("재고 차감 실패: orderNumber={}, reason={}",
                    event.orderNumber(), e.getMessage());

            publishStockFailed(event, e.getMessage());
        }
    }

    private void publishStockDecreased(OrderCreatedEvent event) {
        StockDecreasedEvent result = new StockDecreasedEvent(
                event.orderId(),
                event.orderNumber()
        );

        kafkaTemplate.send(
                KafkaTopic.STOCK_DECREASED,
                String.valueOf(event.orderId()),
                objectMapper.writeValueAsString(result)
        );

        log.info("재고 차감 완료: orderNumber={}", event.orderNumber());
    }

    private void publishStockFailed(OrderCreatedEvent event, String reason) {
        Long failedProductID = event.items().isEmpty()
                ? null
                : event.items().getFirst().productId();

        StockFailedEvent result = new StockFailedEvent(
                event.orderId(),
                event.orderNumber(),
                failedProductID,
                reason
        );

        kafkaTemplate.send(
                KafkaTopic.STOCK_FAILED,
                String.valueOf(event.orderId()),
                objectMapper.writeValueAsString(result)
        );
    }
}
