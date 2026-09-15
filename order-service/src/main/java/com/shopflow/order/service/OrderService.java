package com.shopflow.order.service;

import com.shopflow.common.event.OrderCreatedEvent;
import com.shopflow.common.exception.BusinessException;
import com.shopflow.common.exception.ErrorCode;
import com.shopflow.order.clinet.ProductClient;
import com.shopflow.order.domain.Order;
import com.shopflow.order.domain.OrderItem;
import com.shopflow.order.dto.OrderCreateRequest;
import com.shopflow.order.dto.OrderResponse;
import com.shopflow.order.event.OrderEventProducer;
import com.shopflow.order.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service @Slf4j
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderService {

    private final OrderRepository orderRepository;
    private final ProductClient productClient;
    private final OrderEventProducer orderEventProducer;

    @Transactional
    public OrderResponse createOrder(OrderCreateRequest request, Long userId) {
        List<OrderItem> orderItems = request.items()
                .stream()
                .map(item -> {
                    ProductClient.ProductInfo productInfo
                            = productClient.getProduct(item.productId());
                    return OrderItem.create(
                            item.productId(),
                            productInfo.name(),
                            productInfo.price(),
                            item.quantity()
                    );
                })
                .toList();

        Order saved = orderRepository.save(Order.create(userId, orderItems));

        orderEventProducer.publishOrderCreated(toEvent(saved));

        log.info("주문 생성: orderNumber={}, userId={}", saved.getOrderNumber(), saved.getUserId());

        return OrderResponse.from(saved);
    }

    private OrderCreatedEvent toEvent(Order order) {
        List<OrderCreatedEvent.OrderItemPayload> payloads = order.getOrderItems()
                .stream()
                .map(item -> new OrderCreatedEvent.OrderItemPayload(
                        item.getProductId(),
                        item.getQuantity()
                ))
                .toList();

        return new OrderCreatedEvent(
                order.getId(),
                order.getOrderNumber(),
                order.getUserId(),
                payloads
        );
    }

    public OrderResponse getOrder(Long orderId, Long userId) {
        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new BusinessException(ErrorCode.ORDER_NOT_FOUND));

        if (!order.getUserId().equals(userId)) {
            throw new BusinessException(ErrorCode.ORDER_NOT_FOUND);
        }

        return OrderResponse.from(order);
    }

    public List<OrderResponse> getMyOrders(Long userId) {
        return orderRepository.findByUserIdOrderByDesc(userId)
                .stream()
                .map(OrderResponse::from)
                .toList();
    }

    @Transactional
    public void markAsPaid(Long orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new BusinessException(ErrorCode.ORDER_NOT_FOUND));

        order.markAsPaid();
        log.info("주문 결제 완료: orderNumber={}", order.getOrderNumber());
    }

    @Transactional
    public void cancelOrder(Long orderId, String reason) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new BusinessException(ErrorCode.ORDER_NOT_FOUND));

        order.cancel();
        log.warn("주문 취소: orderNumber={}, reason={}", order.getOrderNumber(), reason);
    }
}
