package com.shopflow.order.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private static final int BATCH_SIZE = 100;

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository.findByStatusOrderByIdAsc(
                OutboxEvent.Status.PENDING, Limit.of(BATCH_SIZE));

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("Outbox 발행 시작: {}건", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {
            try {
                kafkaTemplate.send(
                        event.getEventType(),
                        event.getAggregateId(),
                        event.getPayload()
                ).get();

                event.markAsPublished();

            } catch (Exception e) {
                event.markAsFailed();
                log.error("Outbox 발생 실패: id={}, retryCount={}",
                        event.getId(), event.getRetryCount(), e);
            }
        }
    }

    @Scheduled(cron = "0 0 3 * * *")
    @Transactional
    public void cleanupPublishedEvents() {
        LocalDateTime threshold = LocalDateTime.now().minusDays(7);
        outboxEventRepository.deleteByStatusAndPublishedAtBefore(
                OutboxEvent.Status.PUBLISHED, threshold
        );
        log.info("발행 완료 이벤트 정리: {} 이전", threshold);
    }
}
