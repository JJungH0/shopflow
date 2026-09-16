package com.shopflow.order.service;

import com.shopflow.order.repository.OrderRepository;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.*;

@Slf4j
@SpringBootTest
@Import(TestKafkaConfig.class)
public class DualWriteTest {

    @Autowired
    private OrderFailureSimulator simulator;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private TestEventCollector eventCollector;

    @Test
    @DisplayName("트랜잭션 롤백 시에도 이벤트는 이미 발행되어 데이터 불일치가 발생")
    void dualWrite_rollbackButEventPublished() {
        eventCollector.clear();

        long beforeCount = orderRepository.count();

        assertThatThrownBy(() -> simulator.createOrderAndFail(1L))
                .isInstanceOf(RuntimeException.class);

        long afterCount = orderRepository.count();

        log.info("=== Dual Write 문제 재현 ===");
        log.info("주문 저장 전 건수: " + beforeCount);
        log.info("주문 저장 후 건수: " + afterCount);
        log.info("발행된 이벤트 수: " + eventCollector.size());

        /**
         * 주문은 트랜잭션 처리로 롤백되어 저장되지 않음 기대값 0 - 0
         */
        assertThat(beforeCount).isEqualTo(afterCount);

        /**
         * 문제상황 :
         * 이벤트는 발생되어버림
         */
        assertThat(eventCollector.size()).isEqualTo(1);

    }
}
