/**
 * Photo app load test — k6
 *
 * Mixed workload against photoshare:
 *   upload  10%  POST /photos          — postgres + S3 write
 *   view    50%  GET  /photos/{id}     — postgres read + S3 read
 *   list    30%  GET  /photos?page=N   — postgres paginated query
 *   search  10%  GET  /photos/search   — postgres full-text search
 *
 * Usage:
 *   k6 run --duration 10m --vus 20 scripts/k6/photo-app.ts
 */

import http from "k6/http";
import { check } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://127.0.0.1:8080";

const uploadDuration = new Trend("upload_duration", true);
const viewDuration = new Trend("view_duration", true);
const listDuration = new Trend("list_duration", true);
const searchDuration = new Trend("search_duration", true);
const uploadErrors = new Counter("upload_errors");
const viewErrors = new Counter("view_errors");
const listErrors = new Counter("list_errors");
const searchErrors = new Counter("search_errors");
const errorRate = new Rate("error_rate");

const SEARCH_WORDS = [
  "sunset", "beach", "mountain", "city", "forest", "river", "snow",
  "garden", "portrait", "street", "night", "morning", "autumn", "spring",
];

export const options = {
  scenarios: {
    photo_app: {
      executor: "constant-arrival-rate",
      rate: parseInt(__ENV.RPS || "1000"),
      timeUnit: "1s",
      duration: __ENV.DURATION || "10m",
      preAllocatedVUs: parseInt(__ENV.VUS || "20"),
      maxVUs: parseInt(__ENV.MAX_VUS || "50"),
    },
  },
  thresholds: {
    error_rate: ["rate<0.01"],
    upload_duration: ["p(99)<500"],
    view_duration: ["p(99)<200"],
    list_duration: ["p(99)<100"],
    search_duration: ["p(99)<200"],
  },
};

// Populated by setup(), passed to default function via data arg
let photoIds: string[] = [];

export function setup(): { ids: string[] } {
  const ids: string[] = [];
  console.log("Seeding 200 photos...");
  for (let i = 0; i < 200; i++) {
    const res = http.post(`${BASE}/photos`);
    if (res.status >= 200 && res.status < 300) {
      try {
        ids.push(JSON.parse(res.body as string).id);
      } catch (_) {}
    }
  }
  console.log(`Seeded ${ids.length} photos`);
  return { ids };
}

function pickOp(): string {
  const r = Math.random() * 100;
  if (r < 10) return "upload";
  if (r < 60) return "view";
  if (r < 90) return "list";
  return "search";
}

export default function (data: { ids: string[] }): void {
  // Merge seeded IDs into module-level array once
  if (photoIds.length === 0 && data.ids.length > 0) {
    photoIds = data.ids.slice();
  }
  const op = pickOp();

  switch (op) {
    case "upload": {
      const res = http.post(`${BASE}/photos`);
      uploadDuration.add(res.timings.duration);
      const ok = check(res, {
        "upload 2xx": (r) => r.status >= 200 && r.status < 300,
      });
      if (!ok) {
        uploadErrors.add(1);
        errorRate.add(true);
      } else {
        errorRate.add(false);
        try {
          const body = JSON.parse(res.body as string);
          if (body.id && photoIds.length < 10000) {
            photoIds.push(body.id);
          }
        } catch (_) {}
      }
      break;
    }
    case "view": {
      const id = photoIds.length > 0
        ? photoIds[Math.floor(Math.random() * photoIds.length)]
        : "nonexistent";
      const res = http.get(`${BASE}/photos/${id}`);
      viewDuration.add(res.timings.duration);
      const ok = check(res, {
        "view 2xx or 404": (r) => r.status === 200 || r.status === 404,
      });
      if (!ok) {
        viewErrors.add(1);
        errorRate.add(true);
      } else {
        errorRate.add(false);
      }
      break;
    }
    case "list": {
      const page = Math.floor(Math.random() * 50);
      const res = http.get(`${BASE}/photos?page=${page}`);
      listDuration.add(res.timings.duration);
      const ok = check(res, { "list 2xx": (r) => r.status === 200 });
      if (!ok) {
        listErrors.add(1);
        errorRate.add(true);
      } else {
        errorRate.add(false);
      }
      break;
    }
    case "search": {
      const word = SEARCH_WORDS[Math.floor(Math.random() * SEARCH_WORDS.length)];
      const res = http.get(`${BASE}/photos/search?q=${word}`);
      searchDuration.add(res.timings.duration);
      const ok = check(res, { "search 2xx": (r) => r.status === 200 });
      if (!ok) {
        searchErrors.add(1);
        errorRate.add(true);
      } else {
        errorRate.add(false);
      }
      break;
    }
  }
}
