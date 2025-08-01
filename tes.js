import http from "k6/http";
import { check, sleep } from "k6";
import { Rate } from "k6/metrics";

// Custom error metric
const errorRate = new Rate("errors");

export const options = {
  vus: 20000,
  duration: "5m",
  thresholds: {
    http_req_duration: ["p(95)<500"],
    http_req_failed: ["rate<0.1"],
    errors: ["rate<0.1"],
  },
};

const BASE_URL = "http://localhost:3001";

// Dummy taps generator
function generateTapEvents(count = 5) {
  const taps = [];
  let now = Date.now();

  for (let i = 0; i < count; i++) {
    taps.push({
      t: now,
      v: Math.floor(Math.random() * 10) + 1,
    });
    now += Math.floor(Math.random() * 150) + 50; // jeda antar tap: 50–200ms
  }

  return taps;
}

export default function () {
  const virtualUserId = __VU;
  const userId = 100000 + virtualUserId;

  const taps = generateTapEvents(Math.floor(Math.random() * 10) + 1);
  const payload = JSON.stringify({ taps });

  const params = {
    headers: {
      "Content-Type": "application/json",
      "X-User-Id": userId.toString(),
    },
    timeout: "30s",
  };

  const res = http.post(`${BASE_URL}/api/v1/tap`, payload, params);

  const success = check(res, {
    "status is 200 or 202": (r) => r.status === 200 || r.status === 202,
    "response time < 500ms": (r) => r.timings.duration < 500,
  });

  // Anggap sukses jika status adalah 200 **atau** 202
  const isAcceptable = res.status === 200 || res.status === 202;

  errorRate.add(!(success && isAcceptable));

  if (!isAcceptable) {
    console.log(`❌ Error [user ${userId}] => ${res.status}: ${res.body}`);
  }

  sleep(Math.random() * 4 + 1);
}

export function setup() {
  console.log("🔧 Warming up...");
  http.get(`${BASE_URL}/health`);
  return {};
}

export function teardown() {
  console.log("✅ Load test completed.");
}
