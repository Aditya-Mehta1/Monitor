import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mkdtempDisposableSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { Runtime } from '../src/runtime.mjs';

test('restart finishes an interrupted decision from the saved command record', async t => {
  using dir = mkdtempDisposableSync(join(tmpdir(), 'listener-recovery-'));
  let entered = false;
  const first = new Runtime(dir.path, {
    async review(_change, signal) {
      entered = true;
      await once(signal, 'abort');
      return null;
    },
  }, 60_000);
  let watch;
  try {
    watch = await first.listen({ source: [process.execPath, '-e', 'console.log("Approve by Friday")'], filter: 'review' });
    await t.waitFor(() => assert.ok(entered && first.store.get(watch).state === 'ended'));
    assert.deepEqual(first.store.poll(watch).notices, []);
  } finally {
    await first.close();
  }

  const resumed = new Runtime(dir.path, {
    review: async ({ record }) => ({ message: await readFile(record, 'utf8') }),
  }, 60_000);
  try {
    await resumed.restore();
    await t.waitFor(() => assert.ok(resumed.store.poll(watch).notices.length > 0));
    const { notices } = resumed.store.poll(watch);
    assert.deepEqual(notices.map(({ seq, message }) => [seq, message]), [[1, 'Approve by Friday']]);
  } finally {
    await resumed.close();
  }
});
