import assert from 'node:assert/strict';
import { readRoute, routeHash, commonQuery } from '../src/v3/router.ts';

globalThis.window = { location: { hash: '' } };
const initial = readRoute();
assert.equal(initial.scope, '7d');
assert.equal(initial.mode, 'all');
assert.equal(initial.timezone, 'Asia/Taipei');
assert.equal(commonQuery(initial).get('scope'), '7d');
assert.equal(commonQuery(initial).has('from_ts'), false);
assert.equal(commonQuery(initial).has('to_ts'), false);

for (const scope of ['1h', '6h', 'invalid']) {
  window.location.hash = `#/cycles?scope=${scope}&source=funding&cycle=historical&detail=fills&cursor=retired-page`;
  const route = readRoute();
  assert.equal(route.scope, '7d');
  assert.equal(commonQuery(route).get('scope'), '7d');
  assert.equal(route.source, 'funding');
  assert.equal(route.cycle, 'historical');
  assert.equal(route.detail, 'fills');
  assert.equal(route.cursor, '');
}

for (const scope of ['7d', 'all', 'today', '24h', 'batch', 'custom', 'session']) {
  const selected = { ...initial, page: 'cycles', mode: 'basis', scope,
    source: 'basis', cycle: 'selected', cursor: 'retained-page', batch: 'basis:batch', session: 'selected-session', from: '2026-10-01T00:00', to: '2026-10-10T00:00' };
  window.location.hash = routeHash(selected);
  const restored = readRoute();
  assert.equal(restored.scope, scope);
  assert.equal(restored.page, 'cycles');
  assert.equal(restored.source, 'basis');
  assert.equal(restored.cycle, 'selected');
  assert.equal(restored.cursor, 'retained-page');
  const query = commonQuery(restored, 2_000_000);
  assert.equal(query.get('scope'), ['24h', 'custom'].includes(scope) ? 'custom' : scope);
  assert.equal(query.has('from_ts'), ['24h', 'custom'].includes(scope));
  assert.equal(query.has('to_ts'), ['24h', 'custom'].includes(scope));
  assert.equal(query.has('batch_id'), scope === 'batch');
  assert.equal(query.has('session'), scope === 'session');
}

window.location.hash = '#/overview?scope=session';
assert.equal(readRoute().scope, '7d');
window.location.hash = '#/overview?scope=today';
assert.equal(readRoute().scope, 'today');
assert.ok(routeHash({ ...initial, scope: 'today' }).includes('scope=today'));
assert.ok(!routeHash(initial).includes('scope='));
console.log('PASS: default 7d, unbounded all queries, retired-link fallback, retained scopes and source-qualified navigation');

window.location.hash = '#/losses?source=basis';
assert.equal(readRoute().page, 'cycles');
assert.equal(readRoute().view, 'loss');
window.location.hash = '#/economics?source=funding';
assert.equal(readRoute().page, 'system');
assert.equal(readRoute().evidence, 'research');
window.location.hash = '#/market';
assert.equal(readRoute().chart, 'live');
for (const chart of ['live', 'basis', 'edge']) {
  window.location.hash = routeHash({ ...initial, page:'market', chart });
  assert.equal(readRoute().chart, chart);
}
console.log('PASS: legacy loss / economics links, live default and explicit fixed-q chart');
