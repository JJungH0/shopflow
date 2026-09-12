package com.shopflow.product.dto;

import com.shopflow.product.domain.enums.Category;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

public record ProductCreateRequest(
        @NotBlank String name,
        String description,
        @NotNull @Positive Integer price,
        @NotNull @PositiveOrZero Integer stockQuantity,
        @NotNull Category category) {
}
