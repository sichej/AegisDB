
import http from 'k6/http';
import { check, fail, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:5050';
const MODE = __ENV.MODE || 'single';
const OPERATION = __ENV.OPERATION || 'write';

const VUS = Number(__ENV.VUS || 1);
const ITERATIONS = Number(
  __ENV.ITERATIONS || (OPERATION === 'read' ? 20 : 5)
);

const PREFIX = MODE === 'raft' ? '/admin/kv' : '/kv';
const RUN_ID = __ENV.RUN_ID || 'step136';
const SEED_KEY = `bench-${RUN_ID}-seed`;

// The Raft JSON request shape must match the server DTO.
const RAFT_VALUE_FIELD = __ENV.RAFT_VALUE_FIELD || 'value';

const failures = new Counter('aegis_failures');
const successRate = new Rate('aegis_success_rate');

export const options = {
  scenarios: {
    workload: {
      executor: 'per-vu-iterations',
      vus: VUS,
      iterations: ITERATIONS,
      maxDuration: '2m',
    },
  },

  thresholds: {
    aegis_success_rate: ['rate==1'],
    http_req_failed: ['rate==0'],
  },

  // Avoid retaining large response bodies for write tests.
  discardResponseBodies: false,
};

function isSuccess(response, method) {
  if (method === 'GET') {
    return response.status === 200;
  }

  if (MODE === 'single') {
    return response.status === 204;
  }

  // Accept common successful write receipt status codes.
  return [200, 201, 204].includes(response.status);
}

function writeValue(key, value) {
  const url = `${BASE_URL}${PREFIX}/${encodeURIComponent(key)}`;

  if (MODE === 'single') {
    return http.put(url, value, {
      headers: { 'Content-Type': 'application/octet-stream' },
      timeout: '30s',
      tags: { operation: 'write', engine: MODE },
    });
  }

  const payload = JSON.stringify({
    [RAFT_VALUE_FIELD]: value,
  });

  return http.put(url, payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '30s',
    tags: { operation: 'write', engine: MODE },
  });
}

export function setup() {
  if (!['single', 'raft'].includes(MODE)) {
    fail(`Unsupported MODE: ${MODE}`);
  }

  if (!['read', 'write'].includes(OPERATION)) {
    fail(`Unsupported OPERATION: ${OPERATION}`);
  }

  // Establish a known key before starting the measured workload.
  // Raft leader election may not have finished yet.
  for (let attempt = 0; attempt < 15; attempt++) {
    const response = writeValue(SEED_KEY, 'aegisdb-benchmark');

    if (isSuccess(response, 'PUT')) {
      return;
    }

    if (response.status !== 503) {
      fail(
        `Seed failed: HTTP ${response.status}: ${response.body}`
      );
    }

    sleep(1);
  }

  fail('Server was not ready after 15 attempts.');
}

export default function () {
  const key = OPERATION === 'read'
    ? SEED_KEY
    : `bench-${RUN_ID}-vu${__VU}`;

  let response;
  let expectedMethod;

  if (OPERATION === 'read') {
    expectedMethod = 'GET';

    response = http.get(
      `${BASE_URL}${PREFIX}/${encodeURIComponent(key)}`,
      {
        timeout: '30s',
        tags: { operation: 'read', engine: MODE },
      }
    );
  } else {
    expectedMethod = 'PUT';

    response = writeValue(
      key,
      `value-${__VU}-${__ITER}`
    );
  }

  const valid = isSuccess(response, expectedMethod);

  check(response, {
    'expected HTTP status': () => valid,
  });

  successRate.add(valid);

  if (!valid) {
    failures.add(1);
    console.error(
      `${expectedMethod} ${key}: HTTP ${response.status} ` +
      `${String(response.body).slice(0, 200)}`
    );
  }
}
