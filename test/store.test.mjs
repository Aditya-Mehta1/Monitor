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
  assert.deepEqual(first.notices.map(n => n.seq), [1]);
  assert.deepEqual(store.poll('mail'), first);

  store.capture([{ watch: store.get('mail'), entries: [{ record: 'email' }] }]);
  const second = store.poll('mail', first.next);
  assert.deepEqual(second.notices.map(n => n.seq), [2]);
  assert.deepEqual(store.poll('mail', second.next), { notices: [], next: second.next });
});
