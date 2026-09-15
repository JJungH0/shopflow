import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    stages: [
        { duration: '10s', target: 20 },
        { duration: '30s', target: 20 },
        { duration: '10s', target: 0 },
    ],
    thresholds: {
        http_req_duration: ['p(95)<500'],
        http_req_failed: ['rate<0.01'],
    },
};

const BASE_URL = 'http://localhost:8082';

export function setup() {
    const payload = JSON.stringify({
        name: '부하테스트용 상품',
        description: '캐싱 성능 측정용',
        price: 45000,
        stockQuantity: 999999,
        category: 'ELECTRONICS',
    });

    const res = http.post(`${BASE_URL}/api/products`, payload, {
        headers: { 'Content-Type': 'application/json' },
    });

    const productId = res.json().data;
    console.log(`테스트 대상 상품 ID: ${productId}`);

    return { productId };
}

export default function (data) {
    const res = http.get(`${BASE_URL}/api/products/${data.productId}`);

    check(res, {
        'status is 200': (r) => r.status === 200,
    });

    sleep(0.01);
}
