import assert from 'node:assert/strict';
import { mkdtempDisposableSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { readRecords } from '../src/watchers/command.mjs';

test('stdout chunks yield one verbatim record per complete line', async () => {
  using dir = mkdtempDisposableSync(join(tmpdir(), 'listener-command-'));
  const output = Buffer.from('€30  \n€30  \n\npartial');
  const chunks = [output.subarray(0, 1), output.subarray(1)];
  const records = [];
  await readRecords(chunks, dir.path, entries => records.push(...entries));

  const lines = await Promise.all(records.map(({ record }) => readFile(record, 'utf8')));
  assert.deepEqual(lines, ['€30  ', '€30  ', '']);
});
