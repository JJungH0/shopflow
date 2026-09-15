import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    stages: [
        { duration: '10s', target: 20 },
        { duration: '30s', target: 20 },
        { duration: '10s', target: 0 },
    ],
    thresholds: {
        http_req_duration: ['p(95)<3000'],
        http_req_failed: ['rate<0.01'],
    },
};

const BASE_URL = 'http://localhost:8082';
const CATEGORIES = ['ELECTRONICS', 'CLOTHING', 'FOOD', 'BOOKS', 'ETC'];

export default function () {
    const category = CATEGORIES[Math.floor(Math.random() * CATEGORIES.length)];

    const res = http.get(`${BASE_URL}/api/products?category=${category}`);

    check(res, {
        'status is 200': (r) => r.status === 200,
    });

    sleep(0.01);
}
