package com.shopflow.user.service;

import com.shopflow.common.exception.BusinessException;
import com.shopflow.common.exception.ErrorCode;
import com.shopflow.user.domain.User;
import com.shopflow.user.dto.LoginRequest;
import com.shopflow.user.dto.SignUpRequest;
import com.shopflow.user.dto.TokenResponse;
import com.shopflow.user.jwt.JwtProvider;
import com.shopflow.user.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Transactional
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtProvider jwtProvider;
    private final RedisTemplate<String, String> redisTemplate;

    public void signUp(SignUpRequest req) {
        if(userRepository.existsByEmail(req.email())) throw new BusinessException(ErrorCode.DUPLICATE_EMAIL);

        User user = User.create(req.email(), passwordEncoder.encode(req.password()), req.name());

        userRepository.save(user);
    }

    public TokenResponse login(LoginRequest req) {
        User user = userRepository.findByEmail(req.email()).orElseThrow(
                () -> new BusinessException(ErrorCode.USER_NOT_FOUND)
        );

        if(!passwordEncoder.matches(req.password(), user.getPassword()))
            throw new BusinessException(ErrorCode.INVALID_PASSWORD);

        String accessToken = jwtProvider.createAccessToken(user.getId(), user.getRole().name());
        String refreshToken = jwtProvider.createRefreshToken(user.getId());

        redisTemplate.opsForValue().set(
                "RT:" + user.getId(),
                refreshToken,
                7, TimeUnit.DAYS
        );

        return new TokenResponse(accessToken, refreshToken);
    }

    public void logout(Long userId, String accessToken) {
        redisTemplate.delete("RT:" + userId);

        redisTemplate.opsForValue().set(
                "BL:" + accessToken,
                "logout",
                30, TimeUnit.MINUTES
        );
    }

}
