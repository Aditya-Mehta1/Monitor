import parcel from '@parcel/watcher'
import { lstat } from 'node:fs/promises'
import { dirname } from 'node:path'

const identity = st => st && `${st.dev}:${st.ino}`

function hasRequestedParent(requests, path) {
  while (path !== dirname(path)) {
    path = dirname(path)
    if (requests.has(path)) return true
  }
  return false
}

// Each outermost requested directory owns one recursive FSEvents stream.
export class MacStreams {
  constructor(state, onHint, onError) {
    this.state = state
    this.onHint = onHint
    this.onError = onError
    this.requests = new Map()
    this.streams = new Map()
    this.tail = Promise.resolve()
    this.overflows = 0
  }

  get stats() {
    let active = 0
    for (const slot of this.streams.values()) if (slot.sub) active++
    return { fseventsStreams: active, fallbackScopes: this.streams.size - active, overflows: this.overflows }
  }

  change(path, delta) {
    if (!path) return this.tail
    const count = this.requests.get(path) ?? 0
    if (count + delta) this.requests.set(path, count + delta)
    else this.requests.delete(path)
    if ((count === 0 || count + delta === 0) && !hasRequestedParent(this.requests, path)) return this.sync()
    return this.tail
  }

  covers(path, st) {
    const exact = this.streams.get(path)
    if (exact && st && exact.key !== identity(st)) {
      this.sync(true)
      return false
    }
    for (let dir = path; ; dir = dirname(dir)) {
      if (this.streams.get(dir)?.sub) return true
      if (dir === dirname(dir)) return false
    }
  }

  sync(check = false) {
    this.check ||= check
    if (!this.queued) {
      this.queued = this.tail.then(async () => {
        this.queued = null
        const check = this.check
        this.check = false
        await this.update(check)
      })
      this.tail = this.queued.catch(this.onError)
    }
    return this.tail
  }

  receive(path, error, events) {
    if (error) {
      // A dropped batch can contain every filename from one or more child directories.
      if (error.message.endsWith('File system must be re-scanned.')) this.overflows++
      this.onHint(path)
      this.onError(error)
      return
    }
    for (const event of events) {
      if (event.path === path && event.type === 'delete') {
        // FSEvents stops that subscription when its watched root disappears.
        this.streams.get(path).key = null
        this.sync(true)
      }
      this.onHint(event.path)
    }
  }

  async update(check) {
    const roots = new Set([...this.requests.keys()].filter(path => !hasRequestedParent(this.requests, path)))
    // Bring replacement coverage up before removing a narrower or broader stream.
    for (const path of roots) {
      const old = this.streams.get(path)
      if (old && !check) continue
      let st
      try { st = await lstat(path, { bigint: true }) }
      catch (error) { if (!['ENOENT', 'ENOTDIR'].includes(error.code)) throw error }
      const key = identity(st)
      if (old?.sub && old.key === key) continue
      await old?.sub?.unsubscribe()
      const slot = { key, sub: null }
      this.streams.set(path, slot)
      if (!st?.isDirectory()) continue
      try {
        slot.sub = await parcel.subscribe(path, (error, events) => {
          if (this.streams.get(path) !== slot) return
          this.receive(path, error, events)
        }, { backend: 'fs-events', ignore: [this.state] })
        if (old) this.onHint(path)
      } catch (error) {
        this.onError(new Error(`FSEvents unavailable for ${path}. Using fs.watch and reconciliation. ${error.message}`))
      }
    }
    for (const [path, slot] of this.streams) {
      if (roots.has(path)) continue
      this.streams.delete(path)
      await slot.sub?.unsubscribe()
    }
  }

  close() {
    this.requests.clear()
    return this.sync()
  }
}
