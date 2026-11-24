import assert from 'node:assert/strict';
import { mkdtempDisposableSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import test from 'node:test';
import { HttpsSources } from '../src/watchers/https.mjs';

test('a shared HTTPS watch skips a 304 and delivers the changed body to both subscribers', async t => {
  using dir = mkdtempDisposableSync(join(tmpdir(), 'listener-https-'));
  const changes = [];
  const errors = [];
  const watcher = new HttpsSources(dir.path, {
    intervalMs: 1,
    onVersion: (ids, after) => changes.push({ ids, after }),
    onError: error => errors.push(error),
  });
  const replies = [[200, '"v1"', 'Friday'], [304, '"v1"'], [200, '"v2"', 'Thursday']];
  const tags = [];
  t.mock.method(watcher.agent, 'addRequest', req => {
    tags.push(req.getHeader('if-none-match'));
    const [status, etag, body] = replies.shift() ?? [304, '"v2"'];
    const res = Readable.from(body ? [Buffer.from(body)] : []);
    res.statusCode = status;
    res.headers = { etag };
    queueMicrotask(() => req.emit('response', res));
  });
  try {
    const url = 'https://example.test/review';
    const baseline = await watcher.attach('deadline', url);
    assert.equal(await watcher.attach('blocker', url), baseline);
    await t.waitFor(() => assert.ok(changes.length > 0));

    assert.deepEqual(errors, []);
    assert.deepEqual(tags.slice(0, 3), [undefined, '"v1"', '"v1"']);
    assert.deepEqual(changes.map(({ ids }) => ids.toSorted()), [['blocker', 'deadline']]);
    assert.equal(await readFile(baseline, 'utf8'), 'Friday');
    assert.equal(await readFile(changes[0].after, 'utf8'), 'Thursday');
  } finally {
    await watcher.close();
  }
});
