import {mkdir, realpath} from 'node:fs/promises';
import {join, parse, resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {parseArgs} from 'node:util';
import {Runtime} from './src/runtime.mjs';
import {serve} from './src/server.mjs';

const {values} = parseArgs({options: {
  state: {type: 'string'}, socket: {type: 'string'}, rules: {type: 'string'}, 'watch-root': {type: 'string'},
  'reconcile-ms': {type: 'string', default: '60000'}, 'rule-workers': {type: 'string', default: '4'},
  'https-interval-ms': {type: 'string', default: '60000'},
  'https-concurrency': {type: 'string'}, 'https-per-origin': {type: 'string'},
}});
if (!values.state) throw new Error('Usage: node daemon.mjs --state DIR [--socket PATH] [--rules MODULE] [--watch-root DIR] [--reconcile-ms N] [--https-interval-ms N] [--https-concurrency N] [--https-per-origin N] [--rule-workers N]');
const interval = Number(values['reconcile-ms']);
const httpsInterval = Number(values['https-interval-ms']);
const ruleWorkers = Number(values['rule-workers']);
if (!Number.isSafeInteger(ruleWorkers) || ruleWorkers < 1) throw new Error('Invalid rule worker count');
if (!Number.isInteger(interval) || interval < 1 || interval > 2_147_483_647) throw new Error('Invalid reconciliation interval');
if (!Number.isInteger(httpsInterval) || httpsInterval < 1 || httpsInterval > 2_147_483_647) throw new Error('Invalid HTTPS refresh interval');
await mkdir(values.state, {recursive: true, mode: 0o700});
const state = await realpath(values.state);
const root = await realpath(values['watch-root'] ?? process.cwd());
// Group native subscriptions within the workspace. Avoid monitoring the whole volume by default.
const watchRoot = root !== parse(root).root || values['watch-root'] ? root : null;
const socket = values.socket ? resolve(values.socket) : join(state, 'listener.sock');
const rules = values.rules ? await import(pathToFileURL(resolve(values.rules))) : {};
const runtime = new Runtime(state, rules, interval, ruleWorkers, watchRoot, {
  intervalMs: httpsInterval,
  concurrency: values['https-concurrency'] === undefined ? undefined : Number(values['https-concurrency']),
  perOrigin: values['https-per-origin'] === undefined ? undefined : Number(values['https-per-origin']),
});
try {
  await runtime.restore();
  const closeServer = await serve(socket, runtime);
  process.on('SIGUSR2', () => console.error(JSON.stringify({paths: runtime.paths.stats})));
  let closing = false;
  async function shutdown() {
    if (closing) return;
    closing = true;
    await runtime.close(closeServer());
  }
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
  console.error(`Listening on ${socket}`);
} catch (error) {
  await runtime.close();
  throw error;
}
