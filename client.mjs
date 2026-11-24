import http from 'node:http';
import {createInterface} from 'node:readline';
import {setTimeout as sleep} from 'node:timers/promises';

function request(socketPath, method, args, signal) {
  return new Promise((resolve, reject) => {
    const req = http.request({socketPath, path: '/' + method, method: 'POST', signal}, async res => {
      if (res.statusCode === 200) return resolve(res);
      try {
        const chunks = [];
        for await (const chunk of res) chunks.push(chunk);
        reject(new Error(Buffer.concat(chunks).toString() || `Listener HTTP ${res.statusCode}`));
      } catch (error) { reject(error); }
    });
    req.on('error', reject);
    req.end(JSON.stringify(args));
  });
}

async function json(socket, method, args, signal) {
  const res = await request(socket, method, args, signal);
  const chunks = [];
  for await (const chunk of res) chunks.push(chunk);
  return JSON.parse(Buffer.concat(chunks));
}

export function connect(socketPath) {
  return {
    listen: spec => json(socketPath, 'listen', {cwd: process.cwd(), ...spec}),
    poll: (watch, after, signal) => json(socketPath, 'poll', {watch, after}, signal),
    stop: watch => json(socketPath, 'stop', {watch}),
    replay(watch, {afterSeq, newestFirst, seq, only, limit, signal} = {}) {
      const response = request(socketPath, 'replay', {watch, afterSeq, newestFirst, seq, only, limit}, signal);
      // A caller can hold a finite replay before starting to iterate it.
      response.catch(() => {});
      return (async function* () {
        const res = await response;
        // Older daemons ignore these selectors and would return unrelated changes.
        if (newestFirst && res.headers['listener-replay-order'] !== 'newest-first') {
          res.destroy();
          throw new Error('Restart the listener daemon to enable newest-first history');
        }
        const rows = createInterface({input: res, crlfDelay: Infinity});
        try {
          for await (const row of rows) yield JSON.parse(row);
        } finally {
          rows.close();
          res.destroy();
        }
      })();
    },
  };
}

// Advance after delivery: a watch sequence for live evidence, an opaque cursor for SDK notices.
export async function follow(socket, watch, after, deliver, signal, {evidence} = {}) {
  while (!signal?.aborted) {
    let page;
    try {
      page = evidence
        ? await json(socket, 'live', {watch, afterSeq: after}, signal)
        : await json(socket, 'wait', {watch, after, untilDone: true}, signal);
    } catch (error) {
      if (signal?.aborted) return;
      if (!['ECONNREFUSED', 'ENOENT', 'ECONNRESET', 'EPIPE'].includes(error.code)) throw error;
      try { await sleep(250, undefined, {signal}); }
      catch (error) { if (signal?.aborted) return; throw error; }
      continue;
    }
    if (page.done) return;
    await deliver(page);
    after = page.next;
  }
}
