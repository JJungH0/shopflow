package com.shopflow.product.dto;

import com.shopflow.product.domain.Product;
import com.shopflow.product.domain.enums.Category;
import com.shopflow.product.domain.enums.Status;

public record ProductResponse (
        Long id,
        String name,
        String description,
        Integer price,
        Integer stockQuantity,
        Category category,
        Status status){

    public static ProductResponse from(Product product) {
        return new ProductResponse(
                product.getId(),
                product.getName(),
                product.getDescription(),
                product.getPrice(),
                product.getStockQuantity(),
                product.getCategory(),
                product.getStatus()
        );
    }
}
