import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const accepted = new Counter('orders_accepted');
const outOfStock = new Counter('orders_out_of_stock');
const unexpected = new Rate('orders_unexpected_response');

export const options = {
  scenarios: {
    checkout: {
      executor: 'ramping-arrival-rate',
      startRate: Number(__ENV.START_RPS || 100),
      timeUnit: '1s',
      preAllocatedVUs: Number(__ENV.PRE_ALLOCATED_VUS || 300),
      maxVUs: Number(__ENV.MAX_VUS || 3000),
      stages: [
        { target: Number(__ENV.TARGET_RPS || 1000), duration: __ENV.RAMP_DURATION || '30s' },
        { target: Number(__ENV.TARGET_RPS || 1000), duration: __ENV.HOLD_DURATION || '2m' },
        { target: 0, duration: '15s' },
      ],
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<250', 'p(99)<1000'],
    http_req_failed: ['rate<0.01'],
    orders_unexpected_response: ['rate<0.01'],
  },
};

const baseURL = __ENV.BASE_URL || 'http://localhost:8080';
const productID = __ENV.PRODUCT_ID || 'sku-phone';

export function setup() {
  const response = http.get(`${baseURL}/v1/products/${productID}`);
  check(response, { 'product endpoint ready': (r) => r.status === 200 });
}

export default function () {
  const identity = `${__VU}-${__ITER}-${Date.now()}`;
  const response = http.post(
    `${baseURL}/v1/orders`,
    JSON.stringify({ productId: productID, quantity: 1 }),
    {
      headers: {
        'Content-Type': 'application/json',
        'X-User-ID': `load-user-${__VU}`,
        'Idempotency-Key': `load-${identity}`,
        'X-Request-ID': `k6-${identity}`,
      },
      tags: { name: 'POST /v1/orders' },
    },
  );
  if (response.status === 202 || response.status === 200) {
    accepted.add(1);
  } else if (response.status === 409) {
    outOfStock.add(1);
  } else {
    unexpected.add(true);
  }
  check(response, {
    'accepted or exhausted': (r) => r.status === 202 || r.status === 200 || r.status === 409,
    'request id returned': (r) => Boolean(r.headers['X-Request-Id']),
  });
  sleep(0.01);
}
