import http from "k6/http";
import { check, sleep } from "k6";
import { Rate } from "k6/metrics";

// Metrik error custom (hanya untuk respons tidak valid)
const errorRate = new Rate("errors");

export const options = {
  vus: 10,
  duration: "1m",
  thresholds: {
    http_req_duration: ["p(95)<500"],
    http_req_failed: ["rate<0.1"],
    errors: ["rate<0.1"], // hanya naik kalau status code bukan 200/202
  },
};

const BASE_URL = "http://localhost:3001";

// ✅ Generator taps + timestamp
function generateTapBatch() {
  const taps = [];
  const tapCount = Math.floor(Math.random() * 10) + 1;

  for (let i = 0; i < tapCount; i++) {
    taps.push({
      count: Math.floor(Math.random() * 10) + 1,
      energy_cost: Math.floor(Math.random() * 5) + 1,
    });
  }

  return {
    taps,
    timestamp: Math.floor(Date.now() / 1000), // UNIX timestamp in seconds
  };
}

export default function () {
  const virtualUserId = __VU;
  const userId = 200000 + virtualUserId;

  const payload = JSON.stringify(generateTapBatch());

  const params = {
    headers: {
      "Content-Type": "application/json",
      "X-User-Id": userId.toString(),
    },
    timeout: "30s",
  };

  const res = http.post(`${BASE_URL}/api/v1/tap`, payload, params);

  const isStatusAcceptable = res.status === 200 || res.status === 202;
  const isFastEnough = res.timings.duration < 500;

  // Error rate hanya naik kalau status code bukan 200/202
  errorRate.add(!isStatusAcceptable);

  if (!isStatusAcceptable) {
    console.log(`❌ Error [user ${userId}] => ${res.status}: ${res.body}`);
  }

  // Tetap simpan metrik performa (latency), walau tidak dicetak
  check(res, {
    "response time < 500ms": () => isFastEnough,
  });

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
