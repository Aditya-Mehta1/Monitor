import http from 'node:http';
import net from 'node:net';
import {once} from 'node:events';
import {chmod, lstat, unlink} from 'node:fs/promises';
import {Readable} from 'node:stream';
import {pipeline} from 'node:stream/promises';

async function removeStaleSocket(path) {
  try {
    if (!(await lstat(path)).isSocket()) throw new Error(`Socket path is already a file: ${path}`);
  } catch (error) {
    if (error.code === 'ENOENT') return;
    throw error;
  }
  const probe = net.connect(path);
  try {
    await once(probe, 'connect');
    throw new Error(`Socket is in use: ${path}`);
  } catch (error) {
    if (error.code !== 'ECONNREFUSED') throw error;
    await unlink(path);
  } finally { probe.destroy(); }
}

export async function serve(socketPath, runtime) {
  const attaching = new Set();
  const server = http.createServer(async (req, res) => {
    try {
      if (req.method !== 'POST') throw new Error('Use POST');
      const chunks = [];
      let size = 0;
      for await (const chunk of req) {
        if ((size += chunk.length) > 1024 * 1024) throw new Error('Watch request exceeds 1 MiB');
        chunks.push(chunk);
      }
      const args = JSON.parse(Buffer.concat(chunks));
      const {watch, after} = args;
      let result;
      switch (req.url) {
        case '/listen': {
          const pending = runtime.listen(args);
          attaching.add(pending);
          try { result = await pending; } finally { attaching.delete(pending); }
          break;
        }
        case '/stop': result = await runtime.stop(watch); break;
        case '/replay': {
          const changes = runtime.store.replay(watch, args.afterSeq, args.limit);
          res.setHeader('Content-Type', 'application/x-ndjson');
          res.flushHeaders();
          function* lines() { for (const change of changes) yield JSON.stringify(change) + '\n'; }
          await pipeline(Readable.from(lines()), res);
          return;
        }
        case '/wait':
        case '/poll': {
          const untilDone = req.url === '/wait' && args.untilDone === true;
          const readPage = () => runtime.store.poll(watch, after, untilDone);
          result = readPage();
          if (req.url === '/wait' && !result.notices.length && !result.done) {
            const event = 'notice:' + watch;
            const wake = () => res.end(JSON.stringify(readPage()));
            runtime.once(event, wake);
            res.once('close', () => runtime.off(event, wake));
            return;
          }
          break;
        }
        default: throw new Error('Unknown listener call');
      }
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify(result));
    } catch (error) {
      if (res.destroyed) return;
      if (res.headersSent) return res.destroy(error);
      res.writeHead(400);
      res.end(error.message);
    }
  });
  await removeStaleSocket(socketPath);
  server.listen(socketPath);
  await once(server, 'listening');
  await chmod(socketPath, 0o600);
  return async () => {
    const closed = new Promise(resolve => server.close(resolve));
    server.closeAllConnections();
    await closed;
    await Promise.allSettled(attaching);
  };
}
