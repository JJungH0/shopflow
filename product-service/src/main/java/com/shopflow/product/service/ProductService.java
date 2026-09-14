package com.shopflow.product.service;

import com.shopflow.common.exception.BusinessException;
import com.shopflow.common.exception.ErrorCode;
import com.shopflow.product.domain.Product;
import com.shopflow.product.domain.enums.Category;
import com.shopflow.product.dto.ProductCreateRequest;
import com.shopflow.product.dto.ProductResponse;
import com.shopflow.product.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ProductService {

    private final ProductRepository productRepository;

    @Transactional
    public Long createProduct(ProductCreateRequest req) {
        Product product = Product.create(
                req.name(),
                req.description(),
                req.price(),
                req.stockQuantity(),
                req.category()
        );

        Product saved = productRepository.save(product);
        return saved.getId();
    }

    public ProductResponse getProduct(Long id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new BusinessException(ErrorCode.PRODUCT_NOT_FOUND));

        return ProductResponse.from(product);
    }

    public List<ProductResponse> getProductsByCategory(Category category) {
        return productRepository.findByCategory(category)
                .stream()
                .map(ProductResponse::from)
                .toList();
    }

    @Transactional
    public void decreaseStock(Long productId, int quantity){
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new BusinessException(ErrorCode.PRODUCT_NOT_FOUND));

        product.decreaseStock(quantity);
    }

    @Retryable(
            retryFor = ObjectOptimisticLockingFailureException.class,
            maxAttempts = 100,
            backoff = @Backoff(delay = 50)
    )
    @Transactional
    public void decreaseStockWithRetry(Long productId, int quantity) {
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new BusinessException(ErrorCode.PRODUCT_NOT_FOUND));

        product.decreaseStock(quantity);
    }

    @Transactional
    public void decreaseStockWithPessimisticLock(Long productId, int quantity) {
        Product product = productRepository.findByIdWithPessimisticLock(productId)
                .orElseThrow(() -> new BusinessException(ErrorCode.PRODUCT_NOT_FOUND));

        product.decreaseStock(quantity);
    }

    @Transactional
    public void decreaseStockWithRedisLock(Long productId, int quantity) {
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new BusinessException(ErrorCode.PRODUCT_NOT_FOUND));

        product.decreaseStock(quantity);
    }
}
