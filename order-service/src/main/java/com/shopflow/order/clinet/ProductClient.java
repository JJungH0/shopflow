package com.shopflow.order.clinet;

import com.shopflow.common.exception.BusinessException;
import com.shopflow.common.exception.ErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Objects;

@Slf4j
@Component
public class ProductClient {

    private final RestClient restClient;

    public ProductClient(@Value("${product-service.url}") String baseUrl) {
        this.restClient = RestClient.builder()
                .baseUrl(baseUrl)
                .build();
        log.info("{}",baseUrl);
    }

    public ProductInfo getProduct(Long productId) {
        try {
            ApiResponseWrapper response = restClient.get()
                    .uri("/api/products/{id}", productId)
                    .retrieve()
                    .body(ApiResponseWrapper.class);

            if (Objects.isNull(response) || Objects.isNull(response.data())) {
                throw new BusinessException(ErrorCode.PRODUCT_NOT_FOUND);
            }
            return response.data();
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            log.error("상품 조회 실패: productId={}", productId, e);
            throw new BusinessException(ErrorCode.PRODUCT_SERVICE_UNAVAILABLE);
        }
    }

    public record ProductInfo(
            Long id,
            String name,
            Integer price,
            Integer stockQuantity
    ){}

    private record ApiResponseWrapper(
            boolean success,
            ProductInfo data,
            String message
    ){}
}
