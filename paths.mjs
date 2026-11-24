import { watch as watchDirectory } from 'node:fs'
import { lstat, readdir, realpath } from 'node:fs/promises'
import { basename, dirname, join, sep } from 'node:path'
import { forEachIO, prepareEvidence, saveFile, saveTree } from './evidence.mjs'

const MacStreams = process.platform === 'darwin' ? (await import('./macos.mjs')).MacStreams : null
const MAX_ACTIVE_ROOTS = 8
const missing = error => ['ENOENT', 'ENOTDIR'].includes(error.code)
const contains = (dir, path) => dir === path || path.startsWith(dir.endsWith(sep) ? dir : dir + sep)
const overlaps = (a, b) => contains(a, b) || contains(b, a)

async function statPath(path) {
  try {
    return await lstat(path, { bigint: true })
  } catch (error) {
    if (!missing(error)) throw error
  }
}

// A restored watch may have lost its entire parent directory while offline.
async function canonical(path) {
  try {
    return await realpath(path)
  } catch (error) {
    if (!missing(error) || dirname(path) === path) throw error
    return join(await canonical(dirname(path)), basename(path))
  }
}

export class Paths {
  constructor(stateDir, { onVersions, onError, reconcileMs = 60_000, watchRoot }) {
    this.state = stateDir
    this.watchRoot = watchRoot
    this.mac = MacStreams && new MacStreams(stateDir, path => this.addHint(path), onError)
    this.onVersions = onVersions
    this.onError = onError
    this.io = { filesRead: 0, bytesRead: 0, bytesWritten: 0, reconciliations: 0 }
    this.watches = new Map()
    this.routes = new Map()
    this.canonicalState = realpath(stateDir)
    this.nodes = new Map()
    this.handles = new Map()
    this.unavailable = new Set()
    this.hints = new Set()
    this.ready = prepareEvidence(stateDir)
    this.jobs = new Set()
    this.active = 0
    this.pending = new Map()
    this.timer = setInterval(() => this.reconcile(), reconcileMs)
    this.timer.unref()
  }

  get stats() {
    const mac = this.mac?.stats
    let backend = this.mac ? 'macOS FSEvents' : 'fs.watch'
    if (mac?.fallbackScopes) {
      const fallback = this.handles.size ? 'fs.watch fallback' : 'reconciliation fallback'
      backend = mac.fseventsStreams ? backend + ' + ' + fallback : fallback
    }
    return { ...this.io, backend, ...mac,
      fsWatchDirectories: this.handles.size, nativeDirectories: this.handles.size + (mac?.fseventsStreams ?? 0),
      pathWatches: this.watches.size }
  }

  // Overlapping trees stay in arrival order. Disjoint roots share the I/O budget.
  run(root, task) {
    const job = { root, task, ...Promise.withResolvers() }
    this.jobs.add(job)
    this.startJobs()
    return job.promise
  }

  startJobs() {
    const earlier = []
    for (const job of this.jobs) {
      if (this.active === MAX_ACTIVE_ROOTS) break
      if (!job.running && !earlier.some(root => overlaps(root, job.root))) {
        job.running = true
        this.active++
        this.ready.then(job.task).then(job.resolve, job.reject).finally(() => {
          this.jobs.delete(job)
          this.active--
          this.startJobs()
        })
      }
      earlier.push(job.root)
    }
  }

  addHint(path) {
    if (this.closed || (!this.routes.has(path) && !this.covering(path))) return
    this.hints.add(path)
    if (this.hintTimer) return
    this.hintTimer = setTimeout(() => {
      this.hintTimer = null
      const hints = [...this.hints]
      this.hints.clear()
      this.refresh(hints).catch(this.onError)
    }, 5)
  }

  async attach(id, path, previous) {
    const stat = await statPath(path)
    const source = stat && !stat.isSymbolicLink()
      ? await canonical(path)
      : join(await canonical(dirname(path)), basename(path))
    const state = await this.canonicalState
    if (overlaps(source, state)) throw new Error('Store listener state outside the watched source')
    // A tree gets one stream; a file uses its parent so name replacement remains visible.
    let scope = this.watchRoot && contains(this.watchRoot, source) ? this.watchRoot
      : stat?.isDirectory() ? source : dirname(source)
    if (scope === sep && scope !== this.watchRoot) scope = null
    const registration = { id, path: source, evidence: undefined, scope }
    this.watches.set(id, registration)
    const route = this.route(source)
    route.watches.add(registration)
    const root = this.covering(source).path
    const subscribed = this.mac?.change(scope, 1)
    try {
      return await this.run(root, async () => {
        await subscribed
        await this.ensureParent(dirname(source))
        await this.capture(root, root === source ? null : [source])
        const ref = this.nodes.get(source)?.ref ?? null
        if (!ref && previous === undefined) {
          throw new Error(`Watch an existing regular file or directory: ${source}`)
        }
        registration.evidence = ref
        return ref
      })
    } catch (error) {
      this.detach(id)
      throw error
    }
  }

  detach(id) {
    const watch = this.watches.get(id)
    if (!watch) return
    this.watches.delete(id)
    this.mac?.change(watch.scope, -1)
    let route = this.routes.get(watch.path)
    route.watches.delete(watch)
    while (route && !route.watches.size && !route.children.size) {
      this.routes.delete(route.path)
      route.parent?.children.delete(route)
      route = route.parent
    }
    this.run(watch.path, () => {
      if (!this.covering(watch.path)) this.prune(watch.path)
      this.releaseHandle(dirname(watch.path))
    }).catch(this.onError)
  }

  async close() {
    this.closed = true
    clearInterval(this.timer)
    clearTimeout(this.hintTimer)
    this.watches.clear()
    this.routes.clear()
    await Promise.allSettled([...this.jobs].map(job => job.promise))
    for (const handle of this.handles.values()) handle.watcher.close()
    this.handles.clear()
    await this.mac?.close()
    this.nodes.clear()
  }

  // Only prefixes leading to a registration appear in this index.
  route(path) {
    let node = this.routes.get(path)
    if (!node) {
      const parent = path === sep ? null : this.route(dirname(path))
      node = { path, parent, children: new Set(), watches: new Set() }
      this.routes.set(path, node)
      parent?.children.add(node)
    }
    return node
  }

  covering(path) {
    while (!this.routes.has(path) && path !== sep) path = dirname(path)
    let root
    for (let node = this.routes.get(path); node; node = node.parent) {
      if (node.watches.size) root = node
    }
    return root
  }

  *rootsBelow(node) {
    if (!node) return
    if (node.watches.size) yield node.path
    else for (const child of node.children) yield* this.rootsBelow(child)
  }

  prune(path) {
    if (this.routes.get(path)?.watches.size) return
    const node = this.nodes.get(path)
    if (node?.entries) for (const name of node.entries.keys()) this.prune(join(path, name))
    this.nodes.delete(path)
    this.releaseHandle(path)
  }

  releaseHandle(dir) {
    const handle = this.handles.get(dir)
    if (!handle || this.nodes.get(dir)?.entries) return
    for (const child of this.routes.get(dir)?.children ?? []) if (child.watches.size) return
    handle.watcher.close()
    this.handles.delete(dir)
  }

  async ensureParent(parent) {
    const st = await statPath(parent)
    if (st?.isDirectory()) this.watchDir(parent, st)
  }

  watchDir(dir, st) {
    if (this.closed || this.unavailable.has(dir)) return
    if (this.mac?.covers(dir, st)) {
      this.handles.get(dir)?.watcher.close()
      this.handles.delete(dir)
      return
    }
    const key = `${st.dev}:${st.ino}`
    const old = this.handles.get(dir)
    if (old?.key === key) return
    old?.watcher.close()
    this.handles.delete(dir)
    try {
      const watcher = watchDirectory(dir, (_, name) => {
        this.addHint(name && name !== basename(dir) ? join(dir, name) : dir)
      })
      this.handles.set(dir, { watcher, key })
      watcher.on('error', error => {
        watcher.close()
        if (this.handles.get(dir)?.watcher === watcher) this.handles.delete(dir)
        this.unavailable.add(dir)
        this.onError(error)
        this.addHint(dir)
      })
    } catch (error) {
      this.unavailable.add(dir)
      this.onError(error)
    }
  }

  async reconcile() {
    if (this.scanning || this.closed) return
    this.scanning = true
    try {
      this.unavailable.clear()
      await this.mac?.sync(true)
      const parents = new Set([...this.watches.values()].map(w => dirname(w.path)))
      for (const parent of parents) await this.ensureParent(parent)
      await this.refresh()
      this.io.reconciliations++
    } catch (error) {
      this.onError(error)
    } finally {
      this.scanning = false
    }
  }

  async refresh(hints) {
    const jobs = new Set()
    if (!hints) {
      for (const root of this.rootsBelow(this.routes.get(sep))) jobs.add(this.queueCapture(root))
    } else for (const hint of hints) {
      const root = this.covering(hint)
      if (root) jobs.add(this.queueCapture(root.path, hint))
      else for (const path of this.rootsBelow(this.routes.get(hint))) jobs.add(this.queueCapture(path))
    }
    await Promise.all(jobs)
  }

  queueCapture(root, hint) {
    // While a capture runs, retain at most one further pass per root.
    let batch = this.pending.get(root)
    if (!batch) {
      batch = { edits: new Set() }
      this.pending.set(root, batch)
      batch.done = this.run(root, async () => {
        this.pending.delete(root)
        await this.capture(root, batch.edits && [...batch.edits])
      })
    }
    if (!hint || hint === root) batch.edits = null
    else batch.edits?.add(hint)
    return batch.done
  }

  async capture(root, edits) {
    if (!this.routes.has(root)) return
    const observed = new Set()
    await this.scan(root, edits, observed)
    const versions = []
    for (const path of observed) {
      const after = this.nodes.get(path)?.ref ?? null
      for (const watch of this.routes.get(path)?.watches ?? []) {
        if (watch.evidence !== undefined && after !== watch.evidence) versions.push({ id: watch.id, after })
      }
    }
    if (!versions.length) return
    await this.onVersions(versions)
    for (const { id, after } of versions) {
      const watch = this.watches.get(id)
      if (watch) watch.evidence = after
    }
  }

  forget(path, observed) {
    const route = this.routes.get(path)
    if (route?.watches.size) observed.add(path)
    const old = this.nodes.get(path)
    if (old?.entries) {
      for (const name of old.entries.keys()) this.forget(join(path, name), observed)
      const handle = this.handles.get(path)
      handle?.watcher.close()
      this.handles.delete(path)
    }
    for (const child of route?.children ?? []) {
      if (!old?.entries?.has(basename(child.path))) this.forget(child.path, observed)
    }
    this.nodes.delete(path)
  }

  async scan(path, edits, observed, allowParallel = true) {
    if (this.routes.get(path)?.watches.size) observed.add(path)
    try {
      const st = await lstat(path, { bigint: true })
      const signature = `${st.dev}:${st.ino}:${st.size}:${st.mtimeNs}:${st.ctimeNs}`
      const old = this.nodes.get(path)
      if (st.isFile()) {
        if (old?.signature === signature) return old.ref
        if (old?.entries || this.routes.get(path)?.children.size) this.forget(path, observed)
        const ref = await saveFile(this.state, path, this.io)
        this.nodes.set(path, { signature, ref })
        return ref
      }
      if (st.isDirectory()) {
        this.watchDir(path, st)
        // A directory replacement or a hint for the directory itself needs enumeration.
        const entries = old?.entries ?? new Map()
        const children = new Map()
        let names
        let listed
        let parallelLeaves = false
        if (edits && old?.identity === st.ino) {
          for (const hint of edits) {
            const name = hint.slice(path.length + 1).split(sep, 1)[0]
            if (!children.has(name)) children.set(name, [])
            children.get(name).push(hint)
          }
          names = children.keys()
        } else {
          if (allowParallel && !old) {
            const items = await readdir(path, { withFileTypes: true })
            listed = new Set(items.map(item => item.name))
            parallelLeaves = items.length > 1 && items.every(item => item.isFile())
          } else listed = new Set(await readdir(path))
          names = new Set([...listed, ...entries.keys()])
          // A newly attached parent can cover file watches whose sources already disappeared.
          for (const child of this.routes.get(path)?.children ?? []) names.add(basename(child.path))
        }
        let updated
        const visit = async name => {
          const child = join(path, name)
          const hints = children.get(name)
          let ref = null
          if (listed && !listed.has(name)) this.forget(child, observed)
          else ref = await this.scan(child, hints && !hints.includes(child) ? hints : null, observed, allowParallel && !parallelLeaves)
          if (ref === (entries.get(name) ?? null)) return
          updated ??= new Map(entries)
          if (ref) updated.set(name, ref)
          else updated.delete(name)
        }
        if (parallelLeaves) await forEachIO(names, Math.min(4, names.size), visit)
        else for (const name of names) await visit(name)
        const ref = !old?.entries || updated ? await saveTree(this.state, updated ?? entries) : old.ref
        this.nodes.set(path, { identity: st.ino, entries: updated ?? entries, ref })
        return ref
      }
      // Descendant symlinks and special files have no captured content.
    } catch (error) {
      if (!missing(error) && error.code !== 'ELOOP') throw error
      // A missing evidence destination is a storage error, never a source deletion.
      const current = await statPath(path)
      if (current && !current.isSymbolicLink()) throw error
    }
    this.forget(path, observed)
    return null
  }
}
