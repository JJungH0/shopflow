package com.shopflow.gateway.filter;

import com.shopflow.gateway.jwt.JwtValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

@Slf4j @Component
@RequiredArgsConstructor
public class JwtAuthenticationFilter implements GlobalFilter, Ordered {

    private final JwtValidator jwtValidator;

    private static final List<String> PUBLIC_PATHS = List.of(
            "/api/users/signup",
            "/api/users/login"
    );

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest req = exchange.getRequest();
        String path = req.getURI().getPath();

        if (isPublicPath(path)) {
            return chain.filter(exchange);
        }

        String token = resolveToken(req);

        if (Objects.isNull(token) || !jwtValidator.isValid(token)) {
            log.warn("인증 실패: path={}", path);
            return unauthorized(exchange);
        }

        ServerHttpRequest mutatedReq = req.mutate()
                .header("X-User-Id", jwtValidator.getUserId(token))
                .header("X-User-Role", jwtValidator.getRole(token))
                .build();

        return chain.filter(exchange.mutate().request(mutatedReq).build());
    }

    @Override
    public int getOrder() {
        return -1;
    }

    private Mono<Void> unauthorized(ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        return exchange.getResponse().setComplete();
    }

    private boolean isPublicPath(String path) {
        return PUBLIC_PATHS
                .stream()
                .anyMatch(path::startsWith);
    }

    private String resolveToken(ServerHttpRequest req) {
        String bearer = req.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (Objects.nonNull(bearer) && bearer.startsWith("Bearer ")) {
            return bearer.substring(7);
        }
        return null;
    }

}
