package com.shopflow.order.outbox;


import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    List<OutboxEvent> findByStatusOrderByIdAsc(OutboxEvent.Status status, Limit limit);

    void deleteByStatusAndPublishedAtBefore(OutboxEvent.Status status, LocalDateTime publishedAtBefore);

}
