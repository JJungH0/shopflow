package com.shopflow.order.dto;

import com.shopflow.order.domain.Order;
import com.shopflow.order.domain.enums.Status;

import java.time.LocalDateTime;
import java.util.List;

public record OrderResponse(
        Long id,
        String orderNumber,
        Integer totalAmount,
        Status status,
        List<OrderItemResponse> items,
        LocalDateTime createAt
) {
    public record OrderItemResponse(
            Long productId,
            String productName,
            Integer price,
            Integer quantity,
            Integer subtotal
    ) {
    }

    public static OrderResponse from(Order order) {
        List<OrderItemResponse> items = order.getOrderItems().stream()
                .map(item -> new OrderItemResponse(
                        item.getProductId(),
                        item.getProductName(),
                        item.getPrice(),
                        item.getQuantity(),
                        item.getSubtotal()
                ))
                .toList();

        return new OrderResponse(
                order.getId(),
                order.getOrderNumber(),
                order.getTotalAmount(),
                order.getStatus(),
                items,
                order.getCreatedAt()
        );
    }
}
