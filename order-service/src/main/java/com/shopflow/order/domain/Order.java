package com.shopflow.order.domain;

import com.shopflow.order.domain.enums.Status;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Getter @NoArgsConstructor(access = AccessLevel.PROTECTED)
@EntityListeners(AuditingEntityListener.class)
public class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String orderNumber;

    @Column(nullable = false)
    private Long userId;

    @Column(nullable = false)
    private Integer totalAmount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<OrderItem> orderItems = new ArrayList<>();

    @CreatedDate
    @Column(nullable = false)
    private LocalDateTime createdAt;

    public static Order create(Long userId, List<OrderItem> orderItems) {
        if (Objects.isNull(orderItems) || orderItems.isEmpty()) {
            throw new IllegalArgumentException("주문 상품이 최소 1개 이상이어야 합니다.");
        }

        Order order = new Order();
        order.orderNumber = generateOrderNumber();
        order.userId = userId;
        order.totalAmount = 0;
        order.status = Status.PENDING;

        orderItems.forEach(order::addOrderItem);

        return order;
    }

    private void addOrderItem(OrderItem orderItem) {
        orderItems.add(orderItem);
        orderItem.assignOrder(this);
        totalAmount += orderItem.getSubtotal();
    }

    public void cancel() {
        if (this.status == Status.SHIPPED || this.status == Status.DELIVERED) {
            throw new IllegalStateException("배송이 시작된 주문은 취소할 수 없습니다.");
        }
        this.status = Status.CANCELLED;
    }

    public void markAsPaid() {
        if (this.status != Status.PENDING) {
            throw new IllegalStateException("접수 상태의 주문만 결제 처리할 수 있습니다.");
        }
        this.status = Status.PAID;
    }

    private static String generateOrderNumber() {
        return "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }
}
