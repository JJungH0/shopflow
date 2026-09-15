import http from 'k6/http';

export const options = {
    vus: 10,
    iterations: 5000,
};

const BASE_URL = 'http://localhost:8082';
const CATEGORIES = ['ELECTRONICS', 'CLOTHING', 'FOOD', 'BOOKS', 'ETC'];

export default function () {
    const category = CATEGORIES[Math.floor(Math.random() * CATEGORIES.length)];

    const payload = JSON.stringify({
        name: `상품-${__VU}-${__ITER}`,
        description: '부하 테스트용 더미 상품입니다. '.repeat(10),
        price: Math.floor(Math.random() * 100000) + 1000,
        stockQuantity: Math.floor(Math.random() * 1000),
        category: category,
    });

    http.post(`${BASE_URL}/api/products`, payload, {
        headers: { 'Content-Type': 'application/json' },
    });
}
