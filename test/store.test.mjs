import assert from 'node:assert/strict';
import { mkdtempDisposableSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { Store } from '../src/store.mjs';

function openStore(t) {
  const dir = mkdtempDisposableSync(join(tmpdir(), 'listener-store-'));
  const store = new Store(dir.path);
  t.after(() => {
    store.close();
    dir.remove();
  });
  return store;
}

test('a filtered rollout keeps the removed deadline in replay and delivers only the blocker', t => {
  const store = openStore(t);
  store.add('rollout', { filter: 'on_blocker' }, 'baseline.md');
  store.capture([{ watch: store.get('rollout'), entries: [
    { after: 'friday.md' }, { after: 'baseline.md' }, { after: 'blocked.md' },
  ] }]);
  store.decide(store.pending(), [null, null, { message: 'Blocked: SSO approval missing.' }]);

  const history = [...store.replay('rollout')];
  assert.deepEqual(history.map(({ before, after }) => [before, after]), [
    ['baseline.md', 'friday.md'],
    ['friday.md', 'baseline.md'],
    ['baseline.md', 'blocked.md'],
  ]);
  assert.deepEqual(store.poll('rollout').notices.map(({ seq, message }) => [seq, message]), [
    [3, 'Blocked: SSO approval missing.'],
  ]);
});

test('readers can poll independently and resume after their last notice', t => {
  const store = openStore(t);
  store.add('mail', {});
  store.capture([{ watch: store.get('mail'), entries: [{ record: 'email' }] }]);
  const first = store.poll('mail');
  assert.equal(typeof first.next, 'string');
  assert.deepEqual(first.notices, [{id: 'mail:1', watch: 'mail', seq: 1}]);
  assert.deepEqual(store.poll('mail'), first);
  store.add('other', {});
  store.capture([{watch: store.get('other'), entries: [{record: 'another email'}]}]);
  assert.throws(() => store.poll('other', first.next), /cursor/i);

  store.capture([{ watch: store.get('mail'), entries: [{ record: 'email' }] }]);
  const second = store.poll('mail', first.next);
  assert.deepEqual(second.notices.map(n => n.seq), [2]);
  assert.deepEqual(store.poll('mail', second.next), { notices: [], next: second.next });
});

test('newest-first history bounds originals and seeks through sparse matches with their evidence', t => {
  const store = openStore(t);
  store.add('feed', { filter: 'review' });
  const entries = Array.from({length: 100}, (_, i) => ({record: `receipt-${i + 1}`}));
  store.capture([{watch: store.get('feed'), entries}]);
  const alerts = seq => store.recentChanges('feed', {only: 'alerts', seq});
  assert.deepEqual(alerts(), []); // Captured, still waiting for decisions.
  const verdicts = entries.map(() => null);
  verdicts[1] = {};
  verdicts[96] = {message: 'Needs review'};
  store.decide(store.pending(), verdicts);
  store.stop('feed');

  const all = [...store.replay('feed')];
  assert.deepEqual(store.recentChanges('feed'), all.slice(-24).reverse());
  assert.deepEqual(store.recentChanges('feed', {seq: [95, 99], limit: 2}), [all[98], all[97]]);
  assert.deepEqual(store.recentChanges('feed', {seq: 2}), [all[1]]);
  assert.deepEqual(store.recentChanges('feed', {seq: 101}), []);
  assert.deepEqual(alerts(), [{...all[96], message: 'Needs review'}, all[1]]);
  assert.deepEqual(alerts([2, 96]), [all[1]]);
  assert.deepEqual(alerts(96), []);
  assert.throws(() => store.recentChanges('feed', {seq: [12, 11]}), /inclusive/);
  assert.throws(() => store.recentChanges('feed', {limit: 65}), /limit/);
});

test('live pages use only the watch sequence through interleaved arrivals', t => {
  const store = openStore(t);
  store.add('feed', {});
  assert.deepEqual(store.live('feed'), {notices: [], next: 0});
  const entries = Array.from({length: 137}, (_, i) => ({record: `record-${i + 1}`}));
  store.capture([{watch: store.get('feed'), entries: entries.slice(0, 7)}]);
  store.add('other', {});
  store.capture([{watch: store.get('other'), entries: [{record: 'elsewhere'}]}]);
  store.capture([{watch: store.get('feed'), entries: entries.slice(7)}]);
  const expected = entries.map(({record}, i) => ({seq: i + 1, path: record}));
  const first = store.live('feed');
  assert.deepEqual(first, {notices: expected.slice(0, 128), next: 128});
  const last = store.live('feed', first.next);
  assert.deepEqual(last, {notices: expected.slice(128), next: 137});
  assert.deepEqual(store.live('feed', 70), {notices: expected.slice(70), next: 137});
  assert.deepEqual(store.live('feed', last.next), {notices: [], next: 137});
  store.stop('feed');
  assert.deepEqual(store.live('feed', last.next), {notices: [], next: 137, done: true});

  for (const invalid of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1, '0', store.poll('feed').next, null]) {
    assert.throws(() => store.live('feed', invalid), `invalid live sequence ${invalid}`);
  }
});

test('late filter verdicts respect the capture sequence and an ended watch drains its pending decisions', t => {
  const store = openStore(t);
  store.add('rollout', {filter: 'review'}, 'baseline.md');
  store.capture([{watch: store.get('rollout'), entries: [{after: 'offline.md'}]}]);
  const floor = store.get('rollout').seq;
  store.capture([{watch: store.get('rollout'), entries: [
    {after: 'blocked.md'}, {after: null}, {after: 'quiet.md'},
  ]}]);
  store.end('rollout');
  store.decide(store.pending(), [{}]); // The offline capture matches after the reader attaches.
  assert.deepEqual(store.live('rollout', floor), {notices: [], next: floor});
  store.decide(store.pending(), [{message: 'Blocked'}, {}]);
  const page = store.live('rollout', floor);
  assert.deepEqual(page, {notices: [
    {seq: 2, path: 'blocked.md', message: 'Blocked'}, {seq: 3, path: null},
  ], next: 3});
  assert.deepEqual(store.live('rollout', page.next), {notices: [], next: 3});
  store.decide(store.pending(), [null]);
  assert.deepEqual(store.live('rollout', page.next), {notices: [], next: 3, done: true});
});
