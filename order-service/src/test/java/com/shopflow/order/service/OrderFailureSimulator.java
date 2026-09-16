package com.shopflow.order.service;

import com.shopflow.common.event.OrderCreatedEvent;
import com.shopflow.order.domain.Order;
import com.shopflow.order.domain.OrderItem;
import com.shopflow.order.event.OrderEventProducer;
import com.shopflow.order.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@RequiredArgsConstructor
public class OrderFailureSimulator {

    private final OrderRepository orderRepository;
    private final OrderEventProducer orderEventProducer;

    @Transactional
    public void createOrderAndFail(Long userId) {
        OrderItem item = OrderItem.create(1L, "테스트 상품", 10000, 1);
        Order order = Order.create(userId, List.of(item));
        Order saved = orderRepository.save(order);

        orderEventProducer.publishOrderCreated(
                new OrderCreatedEvent(
                        saved.getId(),
                        saved.getOrderNumber(),
                        userId,
                        List.of(new OrderCreatedEvent.OrderItemPayload(1L, 1))
                )
        );

        throw new RuntimeException("트랜잭션 커밋 전 실패 시뮬레이션");
    }
}
