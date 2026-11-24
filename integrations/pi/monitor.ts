import { randomUUID } from "node:crypto";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { StringEnum } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { connect, follow } from "../../client.mjs";
import { WatchRegistry } from "./registry.ts";

const argv = Type.Array(Type.String(), { minItems: 1 });
const revision = Type.Integer({ minimum: 1 });
const parameters = Type.Object({
  action: StringEnum(["listen", "history", "list", "stop"] as const),
  name: Type.Optional(Type.String({ minLength: 1, description: "Workspace watch name. New for listen." })),
  source: Type.Optional(Type.Union([Type.String({ minLength: 1 }), argv], { description: "File, directory, or HTTPS URL → first observation sets the baseline. Later changes enter history. [executable, ...args] → each complete stdout line enters history as one record." })),
  filter: Type.Optional(Type.Union([Type.String(), argv], { description: 'Named function or [executable, ...args]. Executable stdin → JSON with saved paths (before and after for versions, record for a stdout line). stdout → null skips the ping. {} sends a ping. {"message":"reason"} sends a ping with the reason from the filter. Default → every change matches.' })),
  until: Type.Optional(Type.String({ description: "Watch duration from registration (30s, 7d)." })),
  seq: Type.Optional(Type.Union([revision, Type.Array(revision, { minItems: 2, maxItems: 2 })], { description: "Change number within this watch, starting at 1. Number → exact change. [first, last] → inclusive range. Default → recent changes." })),
  only: Type.Optional(StringEnum(["alerts"] as const, { description: "alerts → matching changes only." })),
}, { additionalProperties: false });

type Evidence = { before: string | null, after: string | null } | { record: string };
type Saved = { seq: number, message?: string } & Evidence;

function namedChange(name: string, change: Saved) {
  const paths = "record" in change ? { record: change.record } : { before: change.before, after: change.after };
  return { name, seq: change.seq, ...paths, ...(change.message !== undefined && { message: change.message }) };
}

function result(data: unknown) {
  return { content: [{ type: "text" as const, text: JSON.stringify(data) }], details: data };
}

function openSession(pi: ExtensionAPI, ctx: ExtensionContext, socket: string) {
  const registry = new WatchRegistry(socket, ctx.cwd);
  const api = connect(socket);
  const owner = ctx.sessionManager.getSessionId();
  const lifetime = new AbortController();
  const readers = new Map<string, AbortController>();

  let activeWrites = 0;

  function attach(name: string, id: string, start: 0 | "latest" = 0) {
    const reader = new AbortController();
    readers.set(name, reader);
    const signal = AbortSignal.any([lifetime.signal, reader.signal]);
    void follow(socket, id, start, ({ notices }) => {
      if (signal.aborted || !notices.length || registry.get(name)?.stopped) return;
      for (const notice of notices) {
        pi.sendMessage({
          customType: "monitor-notice", content: JSON.stringify({ name, ...notice }),
          display: true,
        }, { triggerTurn: true, deliverAs: "followUp" });
      }
    }, signal, { evidence: true }).catch(error => {
      if (!signal.aborted) pi.sendMessage({
        customType: "monitor-error", display: true,
        content: `Delivery for "${name}" failed: ${error.message}. Saved matches are available in history.`,
      });
    }).finally(() => readers.delete(name));
  }

  for (const { name, id } of registry.owned(owner)) attach(name, id, "latest");

  return {
    registry, api, owner, attach, signal: lifetime.signal,
    async write<T>(work: () => Promise<T>) {
      activeWrites++;
      try { return await work(); }
      finally { if (--activeWrites === 0 && lifetime.signal.aborted) registry.close(); }
    },
    stopped(name: string) {
      registry.markStopped(name);
      readers.get(name)?.abort();
    },
    close() {
      lifetime.abort();
      if (!activeWrites) registry.close();
    },
  };
}

export default function (pi: ExtensionAPI) {
  let session: ReturnType<typeof openSession> | undefined;
  function current(ctx: ExtensionContext) {
    const socket = process.env.LISTENER_SOCKET;
    if (!socket) throw new Error("Set LISTENER_SOCKET to the workspace listener socket before starting Pi.");
    return session ??= openSession(pi, ctx, socket);
  }
  pi.on("session_start", (_event, ctx) => { if (process.env.LISTENER_SOCKET) current(ctx); });
  pi.on("session_shutdown", () => { session?.close(); session = undefined; });

  pi.registerTool({
    name: "Monitor",
    label: "Monitor",
    promptSnippet: "Watch a source for changes and read its history.",
    description: `Watch a file, a directory, an HTTPS URL, or the output of a custom command over time. For example, follow a log, track edits to shared notes, or record incoming mail across accounts.
listen(name, source, filter?, until?) → Start a background watch. The filter selects which saved changes send a ping.
  Ping → {name, seq, path, message?}. While connected, this session receives one ping per new match. Open path for the saved content. Deletion of the watched resource → path is null.
history(name, seq?, only?) → Read up to 24 saved changes, newest first, including changes without a ping. The watch name and seq identify one change.
list() → List workspace watch names.
stop(name) → Stop the watch. Keep its history.`,
    parameters,
    async execute(_id, args, signal, _update, ctx) {
      const s = current(ctx);
      const active = signal ? AbortSignal.any([signal, s.signal]) : s.signal;
      active.throwIfAborted();
      if (args.action === "list") return result({ names: s.registry.names() });
      const { name, source, filter, until, seq, only } = args;
      if (!name?.trim()) throw new Error(`Supply a watch name for ${args.action}.`);
      if (args.action === "listen") {
        if (source === undefined) throw new Error("Supply a source for listen.");
        return s.write(async () => {
          // Claim before launching: two sessions choosing one name start only one producer.
          const claim = "pending:" + randomUUID();
          s.registry.add(name, claim, s.owner);
          let id: string | undefined;
          try {
            id = await s.api.listen({ source, filter, until, cwd: ctx.cwd }) as string;
            active.throwIfAborted();
            s.registry.bind(name, claim, id);
          } catch (error) {
            s.registry.release(name, claim);
            if (id) await s.api.stop(id);
            throw error;
          }
          s.attach(name, id);
          return result({ name });
        });
      }
      const watch = s.registry.get(name);
      if (!watch) throw new Error(`Unknown watch name: ${name}. Use list to see workspace watches.`);
      if (watch.id.startsWith("pending:")) {
        if (args.action !== "stop") throw new Error(`Registration still pending: ${name}`);
        s.registry.release(name, watch.id);
        return result({ name, status: "stopped" });
      }
      if (args.action === "stop") return s.write(async () => {
        const status = await s.api.stop(watch.id);
        s.stopped(name);
        return result({ name, status });
      });
      const changes = [];
      for await (const change of s.api.replay(watch.id, { newestFirst: true, seq, only, signal: active })) {
        changes.push(namedChange(name, change));
      }
      return result({ changes });
    },
  });
}
