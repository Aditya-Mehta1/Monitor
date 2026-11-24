import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {setTimeout as delay} from 'node:timers/promises';
import {openBlob, saveRecord} from './evidence.mjs';

export async function launch(argv, cwd, stdin = 'ignore') {
  const child = spawn(argv[0], argv.slice(1), {cwd, detached: true, stdio: [stdin, 'pipe', 'inherit']});
  const closed = new Promise(resolve => child.once('close', (code, signal) => resolve({code, signal})));
  await once(child, 'spawn');
  return {child, closed};
}

function signalGroup(pid, signal) {
  try { return process.kill(-pid, signal); }
  catch (error) { if (error.code === 'ESRCH') return false; throw error; }
}

export function terminate(proc) {
  return proc.ending ??= stopGroup(proc);
}

async function stopGroup({child, closed}) {
  if (signalGroup(child.pid, 'SIGTERM')) {
    const deadline = performance.now() + 500;
    let timer;
    await Promise.race([closed, new Promise(resolve => { timer = setTimeout(resolve, 500); })]);
    clearTimeout(timer);
    // Redirected descendants can outlive the leader. Stop checking as soon as the group disappears.
    while (signalGroup(child.pid, 0)) {
      const remaining = deadline - performance.now();
      if (remaining <= 0) {
        signalGroup(child.pid, 'SIGKILL');
        break;
      }
      await delay(Math.min(25, remaining));
    }
  }
  await closed;
}

// Frame bytes, so an arbitrary UTF-8 or binary record split across pipe reads survives.
export async function readRecords(stream, state, capture) {
  let partial;
  let batch = [];
  async function flush() {
    const entries = await Promise.all(batch);
    capture(entries);
    batch = [];
  }
  try {
    for await (const chunk of stream) {
      let start = 0;
      let end;
      while ((end = chunk.indexOf(10, start)) !== -1) {
        const bytes = chunk.subarray(start, end);
        const observed_at = new Date().toISOString();
        if (partial) {
          await partial.write(bytes);
          batch.push({record: await partial.publish(), observed_at});
          partial = null;
        } else {
          batch.push(saveRecord(state, bytes).then(record => ({record, observed_at})));
        }
        if (batch.length === 32) await flush();
        start = end + 1;
      }
      if (batch.length) await flush();
      if (start < chunk.length) {
        partial ??= await openBlob(state);
        await partial.write(chunk.subarray(start));
      }
    }
  } finally {
    // EOF without LF is an unfinished record.
    if (partial) await partial.discard();
  }
}

export async function executableRule(argv, cwd, change, signal) {
  const proc = await launch(argv, cwd, 'pipe');
  const {child} = proc;
  const abort = () => { void terminate(proc); };
  if (signal.aborted) abort();
  else signal.addEventListener('abort', abort, {once: true});
  // A rule can exit without reading stdin. Its exit status and stdout decide the result.
  child.stdin.on('error', error => {
    if (error.code !== 'EPIPE' && error.code !== 'ECONNRESET') console.error('Rule stdin:', error.message);
  });
  child.stdin.end(JSON.stringify(change) + '\n');
  const chunks = [];
  const budgetKiB = 64 * (Array.isArray(change) ? change.length : 1);
  let size = 0;
  try {
    for await (const chunk of child.stdout) {
      if ((size += chunk.length) > budgetKiB * 1024) throw new Error(`Rule verdict exceeds ${budgetKiB} KiB`);
      chunks.push(chunk);
    }
    const exit = await proc.closed;
    if (exit.code !== 0) throw new Error(`Rule exited with ${exit.signal || exit.code}`);
    return JSON.parse(Buffer.concat(chunks));
  } finally {
    signal.removeEventListener('abort', abort);
    if (proc.ending || (child.exitCode === null && child.signalCode === null)) await terminate(proc);
  }
}
