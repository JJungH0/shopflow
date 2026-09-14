package com.shopflow.order.domain.enums;

public enum Status {
    /**
     * 주문 접수
     */
    PENDING,
    /**
     * 결제 완료
     */
    PAID,
    /**
     * 배송 중
     */
    SHIPPED,
    /**
     * 배송 완료
     */
    DELIVERED,
    /**
     * 취소
     */
    CANCELLED
}
