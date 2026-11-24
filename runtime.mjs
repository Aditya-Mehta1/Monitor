import {randomUUID} from 'node:crypto';
import {EventEmitter} from 'node:events';
import {resolve} from 'node:path';
import {Paths} from './paths.mjs';
import {Store} from './store.mjs';
import {executableRule, launch, readRecords, terminate} from './processes.mjs';

function argv(value, name) {
  if (!Array.isArray(value) || !value.length || !value[0] || !value.every(s => typeof s === 'string')) {
    throw new Error(`${name} must be a nonempty argument vector`);
  }
  return value;
}

function normalize(input, rules) {
  if ('command' in input || 'when' in input) throw new Error('Use stream instead of command and filter instead of when');
  const {path, stream, cwd = process.cwd(), filter, until} = input;
  if ((path === undefined) === (stream === undefined)) throw new Error('Supply exactly one path or stream');
  const spec = {cwd: resolve(cwd)};
  if (path !== undefined) {
    if (!path || typeof path !== 'string') throw new Error('Expected a file or directory path');
    spec.path = resolve(spec.cwd, path);
  } else spec.stream = argv(stream, 'stream');
  if (filter !== undefined) {
    if (typeof filter === 'string') {
      if (!Object.hasOwn(rules, filter) ||
          (typeof rules[filter] !== 'function' && typeof rules[filter]?.evaluateBatch !== 'function')) {
        throw new Error(`Unknown rule "${filter}". Load its module with --rules.`);
      }
      spec.filter = filter;
    } else if (Array.isArray(filter)) spec.filter = argv(filter, 'filter');
    else throw new Error('filter must be a registered rule name or an executable argument vector');
  }
  if (until !== undefined) {
    const match = /^(\d+)(ms|s|m|h|d)$/.exec(until);
    if (!match) throw new Error('Expected a duration such as 30s or 7d');
    const duration = Number(match[1]) * {ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000}[match[2]];
    spec.expires = Date.now() + duration;
    if (!duration || !Number.isSafeInteger(spec.expires)) throw new Error('Invalid watch lifetime');
  }
  return spec;
}

export class Runtime extends EventEmitter {
  constructor(state, rules, reconcileMs, ruleWorkers = 4, watchRoot = null) {
    super();
    this.state = state;
    this.store = new Store(state);
    this.rules = rules;
    this.ruleWorkers = ruleWorkers;
    this.producers = new Map();
    this.evaluating = new Map();
    this.deadlines = new Map();
    this.paths = new Paths(state, {
      reconcileMs, watchRoot, onVersions: versions => this.captureVersions(versions),
      onError: error => console.error('Path capture:', error.message),
    });
  }

  async listen(input) {
    if (this.closing) throw new Error('Listener is closing');
    const spec = normalize(input, this.rules);
    const id = randomUUID();
    if (spec.path) {
      const baseline = await this.paths.attach(id, spec.path);
      try { this.store.add(id, spec, baseline); }
      catch (error) { this.paths.detach(id); throw error; }
    } else {
      const proc = await launch(spec.stream, spec.cwd);
      try { this.store.add(id, spec); }
      catch (error) { await terminate(proc); throw error; }
      this.consume(id, proc);
    }
    this.arm(id, spec.expires);
    return id;
  }

  async restore() {
    for (const {id, state, spec, current} of this.store.restorable()) {
      if (this.expired(id, spec.expires)) continue;
      if (state === 'active' && spec.path) {
        const after = await this.paths.attach(id, spec.path, current);
        if (current !== after) this.capture([{id, entries: [{after}]}]);
      } else if (state === 'active') {
        try { this.consume(id, await launch(spec.stream, spec.cwd)); }
        catch (error) { this.end(id, {error: error.message}); }
      }
      // Check the absolute deadline again with the restored source registered for cleanup.
      this.arm(id, spec.expires);
    }
    this.schedule();
  }

  expired(id, deadline) {
    if (!deadline || deadline > Date.now()) return false;
    void this.stop(id).catch(error => console.error('Expiry:', error.message));
    return true;
  }

  arm(id, deadline) {
    if (!deadline || this.expired(id, deadline)) return;
    this.deadlines.set(id, setTimeout(() => this.arm(id, deadline),
      Math.min(deadline - Date.now(), 2_147_483_647)));
  }

  disarm(id) {
    clearTimeout(this.deadlines.get(id));
    this.deadlines.delete(id);
  }

  captureVersions(versions) {
    // Share a commit across subscriptions observing the same physical update.
    for (let i = 0; i < versions.length; i += 128) {
      this.capture(versions.slice(i, i + 128).map(({id, after}) => ({id, entries: [{after}]})));
    }
  }

  capture(groups) {
    if (this.closing) return;
    const writes = [];
    let filtered = false;
    for (const {id, entries} of groups) {
      const watch = this.store.get(id);
      if (watch.state !== 'active' || this.expired(id, watch.spec.expires)) continue;
      writes.push({watch, entries});
      if (watch.spec.filter) filtered = true;
    }
    if (!writes.length) return;
    for (const id of this.store.capture(writes)) this.emit('notice:' + id);
    if (filtered) this.schedule();
  }

  consume(id, proc) {
    this.producers.set(id, proc);
    proc.task = (async () => {
      try {
        await readRecords(proc.child.stdout, this.state, entries => this.capture([{id, entries}]));
        const exit = await proc.closed;
        if (!this.closing) this.end(id, exit);
      } catch (error) {
        await terminate(proc);
        if (!this.closing) this.end(id, {error: error.message});
      } finally { this.producers.delete(id); }
    })();
  }

  end(id, exit) {
    this.store.end(id, exit);
    console.error(JSON.stringify({watch: id, producer: exit}));
    if (this.store.get(id).pending === null) {
      this.disarm(id);
      this.emit('notice:' + id);
    }
  }

  async stop(id) {
    this.store.stop(id);
    this.emit('notice:' + id);
    this.disarm(id);
    this.paths.detach(id);
    const evaluation = this.evaluating.get(id);
    evaluation?.controller.abort();
    const proc = this.producers.get(id);
    await Promise.all([evaluation?.task, proc && terminate(proc), proc?.task]);
    return 'stopped';
  }

  schedule() {
    if (!this.closing && !this.scheduled) this.scheduled = setImmediate(() => this.pump());
  }

  pump() {
    this.scheduled = null;
    clearTimeout(this.retry);
    while (this.evaluating.size < this.ruleWorkers) {
      const change = this.store.pending([...this.evaluating.keys()]);
      if (!change) break;
      const {spec} = this.store.get(change.watch);
      if (this.expired(change.watch, spec.expires)) continue;
      const controller = new AbortController();
      const job = {controller};
      this.evaluating.set(change.watch, job);
      job.task = this.evaluate(change, spec, controller.signal);
    }
    if (this.evaluating.size < this.ruleWorkers) {
      const next = this.store.nextRetry([...this.evaluating.keys()]);
      if (next !== null) this.retry = setTimeout(() => this.schedule(), Math.max(1, next - Date.now()));
    }
  }

  async runRule(change, {filter, cwd}, signal) {
    if (Array.isArray(filter)) return [await executableRule(filter, cwd, change, signal)];
    const rule = typeof filter === 'string' ? this.rules[filter] : null;
    if (typeof rule === 'function') return [await rule(change, signal)];

    const changes = this.store.batchFrom(change, 32);
    const count = changes.length;
    // Only watches persisted by an older daemon can carry this executable adapter.
    const verdicts = filter.batchCommand
      ? await executableRule(filter.batchCommand, cwd, changes, signal)
      : await rule.evaluateBatch(changes, signal);
    if (!Array.isArray(verdicts) || verdicts.length !== count) throw new Error('Expected one verdict per input change');
    return verdicts;
  }

  async evaluate(change, spec, signal) {
    const id = change.watch;
    try {
      const verdicts = await this.runRule(change, spec, signal);
      if (signal.aborted || this.expired(id, spec.expires)) return;
      for (const verdict of verdicts) {
        if (verdict !== null && (typeof verdict !== 'object' || Array.isArray(verdict) ||
            (verdict.message !== undefined && typeof verdict.message !== 'string'))) throw new Error('Invalid rule verdict');
      }
      if (this.store.decide(change, verdicts)) this.emit('notice:' + id);
      if (this.deadlines.has(id)) {
        const watch = this.store.get(id);
        if (watch.state === 'ended' && watch.pending === null) this.disarm(id);
      }
    } catch (error) {
      if (!signal.aborted) {
        console.error(`Rule ${id}:${change.seq}:`, error.message);
        this.store.defer(change, Date.now() + 1000);
      }
    } finally {
      this.evaluating.delete(id);
      this.schedule();
    }
  }

  async close() {
    this.closing = true;
    clearImmediate(this.scheduled);
    clearTimeout(this.retry);
    for (const id of this.deadlines.keys()) this.disarm(id);
    const jobs = [...this.evaluating.values()];
    for (const job of jobs) job.controller.abort();
    const producers = [...this.producers.values()];
    await Promise.all([this.paths.close(), ...jobs.map(j => j.task),
      ...producers.map(async p => { await terminate(p); await p.task; })]);
    this.store.close();
  }
}
