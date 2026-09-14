package com.shopflow.product.service;

import com.shopflow.product.lock.RedisLockRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class RedisLockStockService {

    private final RedisLockRepository redisLockRepository;
    private final ProductService productService;

    public void decreaseStock(Long productId, int quantity) {
        while (!redisLockRepository.lock(productId)) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        try{
            productService.decreaseStockWithRedisLock(productId, quantity);
        }finally {
            redisLockRepository.unlock(productId);
        }
    }
}
