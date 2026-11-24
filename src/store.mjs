import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const PAGE = 128;

function unpackWatch({ id, spec, state, current, seq, evaluated, due, exit_info }) {
  const pending = due === null ? null : evaluated + 1;
  const saved = JSON.parse(spec);
  // Restore watches registered before the stream/filter rename.
  if (saved.command !== undefined) saved.stream = saved.command;
  if (saved.when !== undefined) saved.filter = saved.when;
  return { id, spec: saved, state, current, seq, pending, exitInfo: JSON.parse(exit_info) };
}

function unpackChange({ watch, seq, observed_at, before, after, record }) {
  const identity = { watch, seq, observed_at };
  return record === null ? { ...identity, before, after } : { ...identity, record };
}

export class Store {
  constructor(stateDir) {
    // An exclusive lock on a separate database gives the workspace one owner until process exit.
    this.owner = new DatabaseSync(join(stateDir, 'owner.sqlite'));
    try {
      this.owner.exec('BEGIN EXCLUSIVE');
    } catch (error) {
      this.owner.close();
      throw new Error(`Cannot acquire workspace listener lock: ${error.message}`, { cause: error });
    }
    this.db = new DatabaseSync(join(stateDir, 'listener.sqlite'));
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = FULL;
      CREATE TABLE IF NOT EXISTS watches (
        id TEXT PRIMARY KEY, spec TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'active',
        current TEXT, seq INTEGER NOT NULL DEFAULT 0, exit_info TEXT,
        evaluated INTEGER NOT NULL DEFAULT 0, due INTEGER
      );
      CREATE INDEX IF NOT EXISTS work ON watches(due) WHERE due IS NOT NULL;
      CREATE TABLE IF NOT EXISTS changes (
        watch TEXT NOT NULL REFERENCES watches, seq INTEGER NOT NULL, observed_at TEXT NOT NULL,
        before TEXT, after TEXT, record TEXT, PRIMARY KEY (watch, seq)
      ) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS notices (
        pos INTEGER PRIMARY KEY, watch TEXT NOT NULL, seq INTEGER NOT NULL, message TEXT,
        UNIQUE (watch, seq), FOREIGN KEY (watch, seq) REFERENCES changes
      );
      CREATE INDEX IF NOT EXISTS deliveries ON notices(watch, pos);
    `);
    this.sql = this.db.createTagStore();
  }

  transaction(fn) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const result = fn();
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      if (this.db.isTransaction) this.db.exec('ROLLBACK');
      throw error;
    }
  }

  add(id, spec, baseline = null) {
    this.sql.run`INSERT INTO watches(id,spec,current) VALUES (${id},${JSON.stringify(spec)},${baseline})`;
  }

  get(id) {
    const row = this.sql.get`SELECT * FROM watches WHERE id = ${id}`;
    if (!row) throw new Error(`Unknown watch: ${id}`);
    return unpackWatch(row);
  }

  restorable() {
    return this.sql.all`SELECT * FROM watches WHERE state = 'active' OR due IS NOT NULL`.map(unpackWatch);
  }

  capture(groups) {
    return this.transaction(() => {
      const notify = [];
      for (const {watch, entries} of groups) if (this.append(watch, entries)) notify.push(watch.id);
      return notify;
    });
  }

  append(watch, entries) {
    const {id} = watch;
    const filtered = Boolean(watch.spec.filter);
    let { seq, current } = watch;
    for (const entry of entries) {
      const record = entry.record ?? null;
      if (record === null && entry.after === current) continue;
      const before = record === null ? current : null;
      const after = entry.after ?? null;
      if (record === null) current = after;
      seq++;
      const at = entry.observed_at ?? new Date().toISOString();
      this.sql.run`INSERT INTO changes(watch,seq,observed_at,before,after,record)
                   VALUES (${id},${seq},${at},${before},${after},${record})`;
      if (!filtered) this.sql.run`INSERT INTO notices(watch,seq) VALUES (${id},${seq})`;
    }
    if (seq === watch.seq) return false;
    if (filtered) {
      this.sql.run`UPDATE watches SET seq=${seq}, current=${current}, due=coalesce(due,${Date.now()})
                   WHERE id=${id}`;
    } else {
      this.sql.run`UPDATE watches SET seq=${seq}, current=${current}, evaluated=${seq} WHERE id=${id}`;
    }
    return !filtered;
  }

  // Rules settle in sequence. Through "evaluated", a notice means match and its absence means skip.
  pending(excluded = []) {
    const row = this.sql.get`
      SELECT c.* FROM watches w JOIN changes c ON c.watch=w.id AND c.seq=w.evaluated+1
      WHERE w.due <= ${Date.now()} AND w.id NOT IN (SELECT value FROM json_each(${JSON.stringify(excluded)}))
      ORDER BY w.due LIMIT 1`;
    return row && unpackChange(row);
  }

  batchFrom({watch, seq}, limit) {
    return this.sql.all`SELECT * FROM changes WHERE watch=${watch} AND seq>=${seq}
                        ORDER BY seq LIMIT ${limit}`.map(unpackChange);
  }

  decide({ watch, seq }, verdicts) {
    const last = seq + verdicts.length - 1;
    return this.transaction(() => {
      const updated = this.sql.get`
        UPDATE watches SET evaluated=${last}, due=CASE WHEN seq > ${last} THEN ${Date.now()} END
        WHERE id=${watch} AND due IS NOT NULL AND evaluated+1=${seq} AND seq>=${last}
        RETURNING state='ended' AND due IS NULL AS finished`;
      if (!updated) return false;
      let matched = false;
      for (const [i, verdict] of verdicts.entries()) {
        if (verdict === null) continue;
        this.sql.run`INSERT INTO notices(watch,seq,message) VALUES (${watch},${seq + i},${verdict.message ?? null})`;
        matched = true;
      }
      // An ended producer's last skip also releases readers waiting for a notice.
      return matched || Boolean(updated.finished);
    });
  }

  defer({ watch, seq }, untilMs) {
    this.sql.run`UPDATE watches SET due=${untilMs} WHERE id=${watch} AND due IS NOT NULL AND evaluated+1=${seq}`;
  }

  nextRetry(excluded = []) {
    return this.sql.get`SELECT min(due) AS next FROM watches WHERE due IS NOT NULL
                        AND id NOT IN (SELECT value FROM json_each(${JSON.stringify(excluded)}))`.next;
  }

  end(id, exitInfo) {
    this.sql.run`UPDATE watches SET state='ended', exit_info=${JSON.stringify(exitInfo ?? null)}
                 WHERE id=${id} AND state='active'`;
  }

  stop(id) {
    this.get(id);
    this.sql.run`UPDATE watches SET state='stopped', due=NULL WHERE id=${id}`;
    return 'stopped';
  }

  poll(id, after = null, untilDone = false) {
    const watch = this.get(id);
    let pos = 0;
    if (after !== null) {
      if (typeof after !== 'string' || !/^n[1-9]\d*$/.test(after)) throw new Error('Invalid notice cursor');
      pos = Number(after.slice(1));
      if (!Number.isSafeInteger(pos) || !this.sql.get`SELECT 1 FROM notices WHERE pos=${pos} AND watch=${id}`) {
        throw new Error('Unknown cursor for this watch');
      }
    }
    const rows = this.sql.all`SELECT pos,seq,message FROM notices WHERE watch=${id} AND pos>${pos}
                              ORDER BY pos LIMIT ${PAGE}`;
    const notices = rows.map(({ seq, message }) => {
      const notice = { id: `${id}:${seq}`, watch: id, seq };
      if (message !== null) notice.message = message;
      return notice;
    });
    const page = { notices, next: rows.length ? `n${rows.at(-1).pos}` : after };
    if (untilDone && !notices.length && watch.state !== 'active' && watch.pending === null) page.done = true;
    return page;
  }

  replay(id, afterSeq = 0, limit) {
    if (!Number.isSafeInteger(afterSeq) || afterSeq < 0) throw new Error('Expected a nonnegative replay sequence');
    if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 1)) throw new Error('Expected a positive replay limit');
    // Take the high-water mark now, even if the caller delays reading its first row.
    const { seq } = this.get(id);
    return this.readChanges(id, limit === undefined ? seq : Math.min(seq, afterSeq + limit), afterSeq);
  }

  *readChanges(id, highWater, last) {
    while (last < highWater) {
      const rows = this.sql.all`SELECT * FROM changes WHERE watch=${id} AND seq>${last} AND seq<=${highWater}
                                ORDER BY seq LIMIT ${PAGE}`;
      for (const row of rows) yield unpackChange(row);
      last = rows.at(-1).seq;
    }
  }

  close() {
    this.db.close();
    this.owner.close();
  }
}
