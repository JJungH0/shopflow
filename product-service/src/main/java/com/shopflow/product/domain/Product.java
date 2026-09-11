package com.shopflow.product.domain;

import com.shopflow.product.domain.enums.Category;
import com.shopflow.product.domain.enums.Status;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDateTime;

@Entity
@Table(name = "products")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@EntityListeners(AuditingEntityListener.class)
public class Product {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(nullable = false)
    private Integer price;

    @Column(nullable = false)
    private Integer stockQuantity;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Category category;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;

    @Version
    private Long version;

    @CreatedDate
    @Column(updatable = false)
    private LocalDateTime createdAt;

    @LastModifiedDate
    private LocalDateTime updatedAt;

    public static Product create (String name,
                                  String description,
                                  Integer price,
                                  Integer stockQuantity,
                                  Category category) {
        Product product = new Product();
        product.name = name;
        product.description = description;
        product.price = price;
        product.stockQuantity = stockQuantity;
        product.category = category;
        product.status = Status.ON_SALE;

        return product;
    }
    
    public void decreaseStock(int quantity) {
        if (this.stockQuantity < quantity) {
            throw new IllegalArgumentException("재고가 부족합니다.");
        }
        this.stockQuantity -= quantity;
        if (this.stockQuantity == 0) {
            this.status = Status.SOLD_OUT;
        }
    }
    
    public void increaseStock(int quantity) {
        this.stockQuantity += quantity;
        if (this.status == Status.SOLD_OUT) {
            this.status = Status.ON_SALE;
        }
    }

    public void update(String name,
                       String description,
                       Integer price,
                       Category category) {
        this.name = name;
        this.description = description;
        this.price = price;
        this.category = category;
    }
}
