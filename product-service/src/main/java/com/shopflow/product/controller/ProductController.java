package com.shopflow.product.controller;

import com.shopflow.common.response.ApiResponse;
import com.shopflow.product.domain.enums.Category;
import com.shopflow.product.dto.ProductCreateRequest;
import com.shopflow.product.dto.ProductResponse;
import com.shopflow.product.service.ProductService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/products")
@RequiredArgsConstructor
public class ProductController {
    private final ProductService productService;

    @PostMapping
    public ResponseEntity<ApiResponse<Long>> createProduct(
            @RequestBody @Valid ProductCreateRequest req) {
        Long productId = productService.createProduct(req);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ApiResponse.ok(productId));
    }

    @GetMapping
    public ResponseEntity<ApiResponse<List<ProductResponse>>> getProducts(
            @RequestParam Category category) {
        return ResponseEntity.ok(
                ApiResponse.ok(productService.getProductsByCategory(category))
        );
    }

    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<ProductResponse>> getProduct(
            @PathVariable Long id) {
        return ResponseEntity.ok(ApiResponse.ok(productService.getProduct(id)));
    }
}
