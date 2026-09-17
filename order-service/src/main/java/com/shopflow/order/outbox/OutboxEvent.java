package com.shopflow.order.outbox;

import com.shopflow.order.domain.enums.Status;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity @Getter
@Table(name = "outbox_events")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String aggregateType;

    @Column(nullable = false)
    private String aggregateId;

    @Column(nullable = false)
    private String eventType;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;

    @Column(nullable = false)
    private Integer retryCount;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    private LocalDateTime publishedAt;

    public enum Status{
        PENDING,
        PUBLISHED,
        FAILED
    }

    public static OutboxEvent create(String aggregateType,
                                     String aggregateId,
                                     String eventType,
                                     String payload) {
        OutboxEvent event = new OutboxEvent();
        event.aggregateType = aggregateType;
        event.aggregateId = aggregateId;
        event.eventType = eventType;
        event.payload = payload;
        event.status = Status.PENDING;
        event.retryCount = 0;
        event.createdAt = LocalDateTime.now();
        return event;
    }

    public void markAsPublished() {
        this.status = Status.PUBLISHED;
        this.publishedAt = LocalDateTime.now();
    }

    public void markAsFailed() {
        this.retryCount++;
        if (this.retryCount >= 30) {
            this.status = Status.FAILED;
        }
    }
}
