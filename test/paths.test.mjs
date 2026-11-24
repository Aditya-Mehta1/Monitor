import assert from 'node:assert/strict';
import { mkdtempDisposableSync } from 'node:fs';
import { readFile, rename, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { Paths } from '../src/watchers/paths.mjs';

test('atomic replacements reach both watches until the file watch detaches', async t => {
  using state = mkdtempDisposableSync(join(tmpdir(), 'listener-state-'));
  using folder = mkdtempDisposableSync(join(tmpdir(), 'listener-folder-'));
  const file = join(folder.path, 'deadline');
  await writeFile(file, 'Friday');

  const versions = { file: [], folder: [] };
  const paths = new Paths(state.path, {
    onVersions(changes) {
      for (const { ids, after } of changes) {
        for (const id of ids) versions[id].push(after);
      }
    },
    onError: error => t.diagnostic(error.message),
  });
  async function replace(contents) {
    const replacement = join(state.path, 'replacement');
    await writeFile(replacement, contents);
    await rename(replacement, file);
    await paths.reconcile();
  }
  try {
    versions.file.push(await paths.attach('file', file));
    versions.folder.push(await paths.attach('folder', folder.path));
    await replace('Thursday');
    paths.detach('file');
    await replace('Monday');

    assert.deepEqual(await Promise.all(versions.file.map(ref => readFile(ref, 'utf8'))), ['Friday', 'Thursday']);
    const deadlines = versions.folder.map(ref => readFile(join(ref, 'deadline'), 'utf8'));
    assert.deepEqual(await Promise.all(deadlines), ['Friday', 'Thursday', 'Monday']);
  } finally {
    await paths.close();
  }
});
