package com.shopflow.product.service;

import com.shopflow.common.exception.BusinessException;
import com.shopflow.common.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedissonLockStockService {

    private final RedissonClient redissonClient;
    private final ProductService productService;

    public void decreaseStock(Long productId, int quantity) {
        RLock lock = redissonClient.getLock("lock:product:" + productId);

        try {
            boolean acquired = lock.tryLock(10, 3, TimeUnit.SECONDS);

            if (!acquired) {
                log.warn("락 획득 실패: productId = {}", productId);
                throw new BusinessException(ErrorCode.LOCK_ACQUISITION_FAILED);
            }

            productService.decreaseStockWithRedisLock(productId, quantity);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}
