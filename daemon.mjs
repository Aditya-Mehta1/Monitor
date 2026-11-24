import {mkdir, realpath} from 'node:fs/promises';
import {join, parse, resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {parseArgs} from 'node:util';
import {Runtime} from './src/runtime.mjs';
import {serve} from './src/server.mjs';

const {values} = parseArgs({options: {
  state: {type: 'string'}, socket: {type: 'string'}, rules: {type: 'string'}, 'watch-root': {type: 'string'},
  'reconcile-ms': {type: 'string', default: '60000'}, 'rule-workers': {type: 'string', default: '4'},
}});
if (!values.state) throw new Error('Usage: node daemon.mjs --state DIR [--socket PATH] [--rules MODULE] [--watch-root DIR] [--reconcile-ms N] [--rule-workers N]');
const interval = Number(values['reconcile-ms']);
const ruleWorkers = Number(values['rule-workers']);
if (!Number.isSafeInteger(ruleWorkers) || ruleWorkers < 1) throw new Error('Invalid rule worker count');
if (!Number.isInteger(interval) || interval < 1 || interval > 2_147_483_647) throw new Error('Invalid reconciliation interval');
await mkdir(values.state, {recursive: true, mode: 0o700});
const state = await realpath(values.state);
const root = await realpath(values['watch-root'] ?? process.cwd());
// Group native subscriptions within the workspace. Avoid monitoring the whole volume by default.
const watchRoot = root !== parse(root).root || values['watch-root'] ? root : null;
const socket = values.socket ? resolve(values.socket) : join(state, 'listener.sock');
const rules = values.rules ? await import(pathToFileURL(resolve(values.rules))) : {};
const runtime = new Runtime(state, rules, interval, ruleWorkers, watchRoot);
try {
  await runtime.restore();
  const closeServer = await serve(socket, runtime);
  process.on('SIGUSR2', () => console.error(JSON.stringify({paths: runtime.paths.stats})));
  let closing = false;
  async function shutdown() {
    if (closing) return;
    closing = true;
    await closeServer();
    await runtime.close();
  }
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
  console.error(`Listening on ${socket}`);
} catch (error) {
  await runtime.close();
  throw error;
}
