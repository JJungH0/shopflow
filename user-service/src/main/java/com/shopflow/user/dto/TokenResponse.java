package com.shopflow.user.dto;

public record TokenResponse(
        String accessToken,
        String refreshToken
) {
}
