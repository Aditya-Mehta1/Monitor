import { realpathSync } from "node:fs";
import { resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";

type Watch = { name: string, id: string, owner: string, stopped: number };

export class WatchRegistry {
  private db: DatabaseSync;
  private workspace: string;
  private sql: ReturnType<DatabaseSync["createTagStore"]>;

  constructor(socket: string, cwd: string) {
    this.workspace = realpathSync(cwd);
    this.db = new DatabaseSync(resolve(socket) + ".pi.sqlite", { timeout: 5000 });
    this.db.exec(`CREATE TABLE IF NOT EXISTS watches (
      workspace TEXT NOT NULL, name TEXT NOT NULL, id TEXT NOT NULL, owner TEXT NOT NULL,
      stopped INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(workspace, name)
    )`);
    this.sql = this.db.createTagStore();
  }

  get(name: string) {
    return this.sql.get`SELECT * FROM watches WHERE workspace=${this.workspace} AND name=${name}` as Watch | undefined;
  }

  add(name: string, id: string, owner: string) {
    const row = this.sql.run`INSERT INTO watches(workspace,name,id,owner) VALUES(${this.workspace},${name},${id},${owner})
                             ON CONFLICT(workspace,name) DO NOTHING`;
    if (!row.changes) throw new Error(`Watch name already in use: ${name}`);
  }

  bind(name: string, claim: string, id: string) {
    const row = this.sql.run`UPDATE watches SET id=${id}
                             WHERE workspace=${this.workspace} AND name=${name} AND id=${claim}`;
    if (!row.changes) throw new Error(`Registration cancelled: ${name}`);
  }

  release(name: string, claim: string) {
    this.sql.run`DELETE FROM watches WHERE workspace=${this.workspace} AND name=${name} AND id=${claim}`;
  }

  names() {
    return this.sql.all`SELECT name FROM watches WHERE workspace=${this.workspace} ORDER BY name`
      .map(row => row.name as string);
  }

  owned(owner: string) {
    return this.sql.all`SELECT * FROM watches WHERE workspace=${this.workspace} AND owner=${owner} AND stopped=0
                        AND id NOT LIKE 'pending:%'` as Watch[];
  }

  markStopped(name: string) {
    this.sql.run`UPDATE watches SET stopped=1 WHERE workspace=${this.workspace} AND name=${name}`;
  }

  close() { this.db.close(); }
}
