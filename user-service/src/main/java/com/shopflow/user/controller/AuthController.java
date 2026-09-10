package com.shopflow.user.controller;

import com.shopflow.common.response.ApiResponse;
import com.shopflow.user.dto.LoginRequest;
import com.shopflow.user.dto.SignUpRequest;
import com.shopflow.user.dto.TokenResponse;
import com.shopflow.user.service.AuthService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class AuthController {
    private final AuthService authService;

    @PostMapping("/signup")
    public ResponseEntity<ApiResponse<Void>> signUp(@RequestBody @Valid SignUpRequest req) {
        authService.signUp(req);
        return ResponseEntity.status(HttpStatus.CREATED).body(ApiResponse.ok());
    }

    @PostMapping("/login")
    public ResponseEntity<ApiResponse<TokenResponse>> login(@RequestBody @Valid LoginRequest req) {
        return ResponseEntity.ok(ApiResponse.ok(authService.login(req)));
    }

    @PostMapping("/logout")
    public ResponseEntity<ApiResponse<Void>> logout(
            @RequestHeader("X-User-Id") Long userId,
            @RequestHeader("Authorization") String bearerToken
    ) {
        String token = bearerToken.substring(7);
        authService.logout(userId, token);
        return ResponseEntity.ok().body(ApiResponse.ok());
    }
}
