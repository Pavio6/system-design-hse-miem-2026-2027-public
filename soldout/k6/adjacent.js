// adjacent: новый путь V3 целиком — join → N соседних мест в секторе 1 → order → pay.
// Все VU целятся в один сидячий сектор (горячий ряд). 409 no_adjacent ожидаем, когда сектор выбран.
// Пороги — лабораторная колонка spec/soldout-v3-adjacent.md.
import { check } from 'k6';
import http from 'k6/http';
import { BASE_URL, EVENT_ID, uuid, join, createOrder, pay, sectorID } from './lib.js';

const json = { 'Content-Type': 'application/json' };

export const options = {
  scenarios: {
    adjacent: {
      executor: 'constant-vus',
      vus: parseInt(__ENV.ADJACENT_VUS || '40', 10),
      duration: __ENV.ADJACENT_DURATION || '45s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{name:adjacent}': ['p(99)<800'],
    'http_req_duration{name:order}': ['p(99)<500'],
    'http_req_duration{name:pay}': ['p(99)<800'],
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  const userId = uuid();
  const token = join(userId);
  if (!token) return;

  const count = 4;
  const res = http.post(
    `${BASE_URL}/v1/holds/adjacent`,
    JSON.stringify({ event_id: EVENT_ID, sector_id: sectorID(1), user_id: userId, count }),
    {
      headers: Object.assign({ 'X-Admission-Token': token, 'Idempotency-Key': uuid() }, json),
      tags: { name: 'adjacent' },
    },
  );
  if (res.status === 409) {
    check(res, { 'adjacent 409 (нет отрезка) — допустимо': (r) => r.status === 409 });
    return;
  }
  const ok = check(res, { 'adjacent 201': (r) => r.status === 201 });
  if (!ok) return;

  const group = res.json();
  check(group, {
    'отрезок из count мест одного ряда': (g) =>
      g.count === count && g.row > 0 && (g.hold_ids || []).length === count && (g.seat_ids || []).length === count,
  });

  const o = createOrder(userId, group.hold_ids);
  check(o, { 'order 201': (r) => r.status === 201 });
  if (o.status !== 201) return;

  const p = pay(o.json('id'), uuid());
  check(p, { 'pay 200 → paid': (r) => r.status === 200 && r.json('status') === 'paid' });
}
