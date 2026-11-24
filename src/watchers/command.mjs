import {openBlob, saveRecord} from '../evidence.mjs';

// Frame bytes, so an arbitrary UTF-8 or binary record split across pipe reads survives.
export async function readRecords(stream, state, capture) {
  let partial;
  let batch = [];
  async function flush() {
    const entries = await Promise.all(batch);
    capture(entries);
    batch = [];
  }
  try {
    for await (const chunk of stream) {
      let start = 0;
      let end;
      while ((end = chunk.indexOf(10, start)) !== -1) {
        const bytes = chunk.subarray(start, end);
        const observed_at = new Date().toISOString();
        if (partial) {
          await partial.write(bytes);
          batch.push({record: await partial.publish(), observed_at});
          partial = null;
        } else {
          batch.push(saveRecord(state, bytes).then(record => ({record, observed_at})));
        }
        if (batch.length === 32) await flush();
        start = end + 1;
      }
      if (batch.length) await flush();
      if (start < chunk.length) {
        partial ??= await openBlob(state);
        await partial.write(chunk.subarray(start));
      }
    }
  } finally {
    // EOF without LF is an unfinished record.
    if (partial) await partial.discard();
  }
}
