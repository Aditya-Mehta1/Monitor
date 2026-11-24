# Pi Monitor extension

Wraps the workspace listener into a `Monitor` tool so Pi can name a watch, finish its turn, and receive a message for each new match while connected.

```sh
LISTENER_SOCKET=/path/to/listener.sock pi -e /abs/path/to/listener/integrations/pi/monitor.ts
```

```text
pi/
├── monitor.ts    # registers Monitor and forwards live matches to the owning Pi session
└── registry.ts   # maps workspace watch names to listener IDs and owners in SQLite
```

## `Monitor`

```text
Monitor({action: "listen", name, source, filter?, until?})  → {name}
Monitor({action: "history", name, seq?, only?})             → {changes}
Monitor({action: "list"})                                   → {names}
Monitor({action: "stop", name})                             → {name, status}
```

While connected, the owning session receives one `{name, seq, path, message?}` ping per match. Pi starts a turn when idle or queues the ping when busy. Open `path` for the saved content. If the watched resource is deleted, `path` is `null`.

## Details

- Pi names each watch. For example, `blockers` and `deadlines` can both watch `rollout.md`. One filter detects new blockers. The other detects deadline changes.
- Each ping names the watch that matched. Stopping `blockers` leaves `deadlines` running.
- Only the session that started a watch receives its pings. Any Pi session in the same workspace can list the watch or read its history by name.
