import { Agent, get } from 'node:https'
import { performance } from 'node:perf_hooks'
import { openBlob } from '../evidence.mjs'

// Each source is either waiting for its deadline, ready at its origin, or running.
// Unlinking a stopped source and taking the next item both take constant time.
class Queue {
  add(item) {
    if (item.queue === this) return
    item.queue?.delete(item)
    item.queue = this
    item.prev = this.tail
    if (this.tail) this.tail.next = item
    else this.head = item
    this.tail = item
  }

  delete(item) {
    if (item.queue !== this) return
    if (item.prev) item.prev.next = item.next
    else this.head = item.next
    if (item.next) item.next.prev = item.prev
    else this.tail = item.prev
    item.queue = item.next = item.prev = null
  }

  shift() {
    const item = this.head
    this.delete(item)
    return item
  }
}

// Bound idle socket retention separately from the request scheduler. A cold origin
// can still open a socket while an unrelated origin has idle sockets pooled.
class WorkspaceAgent extends Agent {
  constructor(concurrency, perOrigin) {
    super({ keepAlive: true, maxSockets: perOrigin, timeout: 30_000 })
    this.poolBudget = concurrency
  }

  keepSocketAlive(socket) {
    return this.totalSocketCount <= this.poolBudget && super.keepSocketAlive(socket)
  }
}

export class HttpsSources {
  constructor(state, { onVersion, onError, intervalMs = 60_000, concurrency = 128, perOrigin = 64 }) {
    for (const value of [intervalMs, concurrency, perOrigin]) {
      if (!Number.isSafeInteger(value) || value < 1) throw new Error('Expected positive HTTPS watcher limits')
    }
    this.state = state
    this.onVersion = onVersion
    this.onError = onError
    this.intervalMs = intervalMs
    this.concurrency = concurrency
    this.perOrigin = perOrigin
    this.agent = new WorkspaceAgent(concurrency, perOrigin)
    this.sources = new Map()
    this.owners = new Map()
    this.origins = new Map()
    this.readyOrigins = new Queue()
    this.deadlines = new Queue()
    this.jobs = new Set()
  }

  async attach(id, address, previous) {
    if (this.closed) throw new Error('HTTPS watcher closed')
    const url = new URL(address)
    if (url.protocol !== 'https:' || url.username || url.password) {
      throw new Error('Expected an HTTPS URL without embedded credentials')
    }
    url.hash = ''
    let source = this.sources.get(url.href)
    if (!source) {
      let group = this.origins.get(url.origin)
      if (!group) {
        group = { origin: url.origin, ready: new Queue(), running: 0, sources: 0 }
        this.origins.set(url.origin, group)
      }
      group.sources++
      source = { url, group, current: null, ids: new Set(), joining: new Map(), reconcile: new Set() }
      this.sources.set(url.href, source)
    }
    this.owners.set(id, source)
    if (previous != null) {
      source.current ??= previous
      if (previous !== source.current) source.reconcile.add(id)
      source.ids.add(id)
      this.enqueue(source)
      return previous
    }
    if (source.current !== null) {
      source.ids.add(id)
      return source.current
    }
    const baseline = Promise.withResolvers()
    source.joining.set(id, baseline)
    this.enqueue(source)
    return baseline.promise
  }

  detach(id, reason = new Error('HTTPS watch stopped')) {
    const source = this.owners.get(id)
    if (!source) return
    this.owners.delete(id)
    source.ids.delete(id)
    source.reconcile.delete(id)
    source.joining.get(id)?.reject(reason)
    source.joining.delete(id)
    if (source.ids.size || source.joining.size) return
    this.sources.delete(source.url.href)
    source.queue?.delete(source)
    const { group } = source
    group.sources--
    if (!group.ready.head) this.readyOrigins.delete(group)
    if (!group.sources && !group.running) this.origins.delete(group.origin)
    source.controller?.abort(reason)
    return source.job
  }

  enqueue(source) {
    if (source.controller || this.closed) return
    const { group } = source
    group.ready.add(source)
    if (group.running < this.perOrigin) this.readyOrigins.add(group)
    this.schedulePump()
  }

  schedulePump() {
    if (!this.scheduled && !this.closed && this.readyOrigins.head && this.jobs.size < this.concurrency) {
      this.scheduled = setImmediate(() => this.pump())
    }
  }

  pump() {
    this.scheduled = null
    while (this.readyOrigins.head && this.jobs.size < this.concurrency) {
      // Reinsert eligible origins at the back for round-robin service.
      const group = this.readyOrigins.shift()
      const source = group.ready.shift()
      group.running++
      if (group.ready.head && group.running < this.perOrigin) this.readyOrigins.add(group)
      const controller = new AbortController()
      source.controller = controller
      const job = this.refresh(source, controller.signal).catch(error => {
        if (controller.signal.aborted) return
        for (const id of source.joining.keys()) this.detach(id, error)
        if (source.ids.size) this.onError(new Error(`HTTPS refresh failed for ${source.url.origin}: ${error.message}`, { cause: error }))
      }).finally(() => {
        this.jobs.delete(job)
        source.job = null
        source.controller = null
        group.running--
        if (group.ready.head) this.readyOrigins.add(group)
        if (!group.sources && !group.running) this.origins.delete(group.origin)
        if (!this.closed && this.sources.get(source.url.href) === source) {
          // All sources have the same interval, so completion order is deadline order.
          source.due = performance.now() + this.intervalMs
          this.deadlines.add(source)
          this.armTimer()
        }
        this.schedulePump()
      })
      source.job = job
      this.jobs.add(job)
    }
  }

  armTimer() {
    if (this.timer || !this.deadlines.head) return
    const due = this.deadlines.head.due
    this.timer = setTimeout(() => {
      this.timer = null
      const now = performance.now()
      while (this.deadlines.head && this.deadlines.head.due <= now) this.enqueue(this.deadlines.head)
      this.armTimer()
    }, Math.max(1, Math.min(2_147_483_647, Math.ceil(due - performance.now()))))
    this.timer.unref()
  }

  async refresh(source, signal) {
    const headers = { 'Accept-Encoding': 'identity' }
    if (source.etag) headers['If-None-Match'] = source.etag
    else if (source.modified) headers['If-Modified-Since'] = source.modified
    const response = await new Promise((resolve, reject) => {
      const request = get(source.url, { agent: this.agent, headers, signal }, res => {
        clearTimeout(headersDue)
        resolve(res)
      })
      const timeout = () => request.destroy(new Error('HTTPS request timed out'))
      const headersDue = setTimeout(timeout, 30_000)
      request.once('error', error => { clearTimeout(headersDue); reject(error) })
      // Large bodies may legitimately take minutes while data continues to arrive.
      request.setTimeout(30_000, timeout)
    })
    let after = source.current
    const h = response.headers
    const unchanged = response.statusCode === 304
    try {
      if (unchanged) {
        if (!source.etag && !source.modified) throw new Error('Unsolicited HTTP 304')
      } else {
        if (response.statusCode !== 200) throw new Error('HTTP ' + response.statusCode)
        if (h['content-encoding'] && h['content-encoding'].toLowerCase() !== 'identity') {
          throw new Error('Expected an identity-encoded HTTPS response')
        }
        const blob = await openBlob(this.state)
        try {
          for await (const chunk of response) await blob.write(chunk)
          signal.throwIfAborted()
          after = await blob.publish()
        } catch (error) {
          await blob.discard()
          throw error
        }
      }
      signal.throwIfAborted()
      const notify = after === source.current ? source.reconcile : source.ids
      if (notify.size) this.onVersion([...notify], after)
      source.current = after
      source.etag = h.etag ?? (unchanged ? source.etag : undefined)
      source.modified = h['last-modified'] ?? (unchanged ? source.modified : undefined)
      source.reconcile.clear()
      for (const [id, waiter] of source.joining) {
        source.ids.add(id)
        waiter.resolve(after)
      }
      source.joining.clear()
    } finally {
      if (unchanged) response.resume()
      else if (!response.readableEnded) response.destroy()
    }
  }

  async close() {
    this.closed = true
    clearImmediate(this.scheduled)
    clearTimeout(this.timer)
    for (const id of this.owners.keys()) this.detach(id, new Error('HTTPS watcher closed'))
    this.agent.destroy()
    await Promise.all([...this.jobs])
  }
}
