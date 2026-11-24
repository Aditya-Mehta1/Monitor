import { createHash, randomUUID } from 'node:crypto'
import { constants } from 'node:fs'
import { access, link, mkdir, mkdtemp, open, rename, rm, symlink, writeFile } from 'node:fs/promises'
import { basename, dirname, join, relative } from 'node:path'

const initialized = new Map()

export function prepareEvidence(state) {
  if (!initialized.has(state)) {
    initialized.set(state, Promise.all(['blobs', 'trees', 'tmp'].map(name =>
      mkdir(join(state, name), { recursive: true, mode: 0o700 }))))
  }
  return initialized.get(state)
}

async function exists(path) {
  try {
    await access(path)
    return true
  } catch (error) {
    if (error.code === 'ENOENT') return false
    throw error
  }
}

// The workspace has one writer process. Share an in-flight publish so concurrent
// arrivals of the same bytes cannot replace an inode already linked into a tree.
const publishing = new Map()

async function install(dest, create) {
  const active = publishing.get(dest)
  if (active) {
    await active
    return false
  }
  const job = (async () => {
    if (await exists(dest)) return false
    await create()
    return true
  })()
  publishing.set(dest, job)
  try {
    return await job
  } finally {
    publishing.delete(dest)
  }
}

export async function saveRecord(state, bytes, stats) {
  await prepareEvidence(state)
  const digest = createHash('sha256').update(bytes).digest('hex')
  const dest = join(state, 'blobs', digest)
  await install(dest, async () => {
    const temp = join(state, 'tmp', randomUUID())
    try {
      await writeFile(temp, bytes, { mode: 0o400 })
      if (stats) stats.bytesWritten += bytes.length
      await rename(temp, dest)
    } catch (error) {
      await rm(temp, { force: true })
      throw error
    }
  })
  return dest
}

// Large files and records split across pipe reads share the same incremental writer.
export async function openBlob(state, stats) {
  await prepareEvidence(state)
  const temp = join(state, 'tmp', randomUUID())
  const out = await open(temp, 'wx', 0o400)
  const hash = createHash('sha256')
  return {
    async write(bytes) {
      hash.update(bytes)
      await out.writeFile(bytes)
      if (stats) stats.bytesWritten += bytes.length
    },
    async publish() {
      await out.close()
      const dest = join(state, 'blobs', hash.digest('hex'))
      if (!await install(dest, () => rename(temp, dest))) await rm(temp)
      return dest
    },
    async discard() {
      await out.close()
      await rm(temp, { force: true })
    },
  }
}

export async function saveFile(state, source, stats) {
  const file = await open(source, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK)
  try {
    const stat = await file.stat()
    if (!stat.isFile()) throw new Error(`Expected a regular file: ${source}`)
    stats.filesRead++
    // A fixed read size also bounds memory when a writer is appending during capture.
    const chunk = Buffer.allocUnsafe(Math.min(stat.size, 512 * 1024))
    if (chunk.length === stat.size) {
      let length = 0
      while (length < chunk.length) {
        const { bytesRead } = await file.read(chunk, length, chunk.length - length)
        if (bytesRead === 0) break
        length += bytesRead
      }
      stats.bytesRead += length
      return await saveRecord(state, chunk.subarray(0, length), stats)
    }

    const blob = await openBlob(state, stats)
    try {
      let remaining = stat.size
      while (remaining > 0) {
        const { bytesRead } = await file.read(chunk, 0, Math.min(chunk.length, remaining))
        if (!bytesRead) break
        remaining -= bytesRead
        await blob.write(chunk.subarray(0, bytesRead))
        stats.bytesRead += bytesRead
      }
      return await blob.publish()
    } catch (error) {
      await blob.discard()
      throw error
    }
  } finally {
    await file.close()
  }
}

// Siblings share the iterator. Finish outstanding I/O before a caller removes a temporary tree.
export async function forEachIO(items, concurrency, visit) {
  const iterator = items[Symbol.iterator]()
  async function work() { for (const item of iterator) await visit(item) }
  const jobs = await Promise.allSettled(Array.from({ length: concurrency }, work))
  for (const job of jobs) if (job.status === 'rejected') throw job.reason
}

// A tree is an ordinary readable directory. Only directories on the edited branch
// get new entries; files are hard linked and child trees are shared by reference.
export async function saveTree(state, entries) {
  const hash = createHash('sha256')
  for (const [name, ref] of [...entries].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)) {
    hash.update(JSON.stringify([name, basename(dirname(ref)), basename(ref)]))
  }
  const dest = join(state, 'trees', hash.digest('hex'))
  if (await exists(dest)) return dest
  const temp = await mkdtemp(join(state, 'tmp', 'tree-'))
  try {
    await forEachIO(entries, Math.min(4, entries.size), async ([name, ref]) => {
      if (basename(dirname(ref)) === 'trees') await symlink(relative(dest, ref), join(temp, name))
      else await link(ref, join(temp, name))
    })
    await rename(temp, dest)
    return dest
  } catch (error) {
    await rm(temp, { recursive: true, force: true })
    // Independent roots can finish an identical tree at the same time.
    if ((error.code === 'EEXIST' || error.code === 'ENOTEMPTY') && await exists(dest)) return dest
    throw error
  }
}
