package com.shopflow.product.service;

import com.shopflow.product.domain.Product;
import com.shopflow.product.domain.enums.Category;
import com.shopflow.product.repository.ProductRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Slf4j
@SpringBootTest
class ProductServiceTest {

    @Autowired
    private ProductService productService;

    @Autowired
    private ProductRepository repository;

    private Long productId;
    @Autowired
    private RedisLockStockService redisLockStockService;
    @Autowired
    private RedissonLockStockService redissonLockStockService;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
        Product product = Product.create(
                "테스트 상품", "동시성 테스용", 10_000, 100, Category.ELECTRONICS
        );

        productId = repository.save(product).getId();
    }

    @Test
    @DisplayName("낙관적 락 + 재시도 : 동시에 100개 요청")
    void decreaseStock_concurrency() throws InterruptedException {
        int threadCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(threadCount);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failCount = new AtomicInteger();

        long start = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    productService.decreaseStockWithRetry(productId, 1);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executorService.shutdownNow();

        long elapsed = System.currentTimeMillis() - start;

        Product product = repository.findById(productId).orElseThrow();

        log.info("성공 : {}", successCount.get());
        log.info("실패 : {}", failCount.get());
        log.info("남은 재고 : {}", product.getStockQuantity());
        log.info("소요 시간 : {}ms", elapsed);

        assertThat(product.getStockQuantity()).isZero();
    }

    @Test
    @DisplayName("비관적 락 : 동시에 100건 요청")
    void decreaseStock_pessimisticLock() throws InterruptedException {
        int threadCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(threadCount);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failCount = new AtomicInteger();

        long start = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    productService.decreaseStockWithPessimisticLock(productId, 1);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executorService.shutdown();

        long elapsed = System.currentTimeMillis() - start;

        Product product = repository.findById(productId).orElseThrow();

        log.info("성공 : {}", successCount.get());
        log.info("실패 : {}", failCount.get());
        log.info("남은 재고 : {}", product.getStockQuantity());
        log.info("소요 시간 : {}ms", elapsed);

        assertThat(product.getStockQuantity()).isZero();
    }

    @Test
    @DisplayName("Redis 분산 락 : 동시 100건 요청")
    void decreaseStock_redisLock() throws InterruptedException {
        int threadCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(threadCount);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failCount = new AtomicInteger();

        long start = System.currentTimeMillis();

        for (int i = 0; i < 100; i++) {
            executorService.submit(() -> {
                try {
                    redisLockStockService.decreaseStock(productId, 1);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executorService.shutdown();

        long elapsed = System.currentTimeMillis() - start;

        Product product = repository.findById(productId).orElseThrow();

        log.info("성공 : {}", successCount.get());
        log.info("실패 : {}", failCount.get());
        log.info("남은 재고 : {}", product.getStockQuantity());
        log.info("소요 시간 : {}ms", elapsed);

        assertThat(product.getStockQuantity()).isZero();
    }

    @Test
    @DisplayName("Redisson 분산 락: 동시 100건 요청")
    void decreaseStock_redissonLock() throws InterruptedException {
        int threadCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(threadCount);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failCount = new AtomicInteger();

        long start = System.currentTimeMillis();

        for (int i = 0; i < 100; i++) {
            executorService.submit(() -> {
                try {
                    redissonLockStockService.decreaseStock(productId, 1);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executorService.shutdown();

        long elapsed = System.currentTimeMillis() - start;

        Product product = repository.findById(productId).orElseThrow();

        log.info("성공 : {}", successCount.get());
        log.info("실패 : {}", failCount.get());
        log.info("남은 재고 : {}", product.getStockQuantity());
        log.info("소요 시간 : {}ms", elapsed);

        assertThat(product.getStockQuantity()).isZero();

    }
}