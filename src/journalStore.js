const fsp = require('fs/promises');
const path = require('path');
const v8 = require('v8');

const MAGIC = 0x4d514a31;

const TYPE_ENQ = 1;
const TYPE_ACK = 2;
const TYPE_DROP_SECOND = 3;

function makeCrcTable() {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let k = 0; k < 8; k++) {
      c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
    }
    table[i] = c >>> 0;
  }
  return table;
}

const CRC_TABLE = makeCrcTable();

function crc32Init() {
  return 0xffffffff;
}

function crc32Update(crc, buf) {
  let c = crc >>> 0;
  for (let i = 0; i < buf.length; i++) {
    c = CRC_TABLE[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
  }
  return c >>> 0;
}

function crc32Final(crc) {
  return (crc ^ 0xffffffff) >>> 0;
}

async function fileDataSync(handle) {
  if (typeof handle.datasync === 'function') {
    await handle.datasync();
    return;
  }
  await handle.sync();
}

class JournalStore {
  constructor({
    baseDir,
    key,
    maxSize,
    trimStrategy,
    durability,
    flushIntervalMs,
    maxLogSizeBytes,
    compactMinAcks,
    compactMinIntervalMs,
  }) {
    this._baseDir = baseDir;
    this._key = key;
    this._dir = path.join(baseDir, `memqueue-${key}`);
    this._filePath = path.join(this._dir, 'queue.log');

    this._maxSize = Number(maxSize) > 0 ? Number(maxSize) : 0;
    this._trimStrategy = trimStrategy;

    this._maxLogSizeBytes = Number(maxLogSizeBytes) > 0 ? Number(maxLogSizeBytes) : 0;

    this._compactMinAcks = Number(compactMinAcks) > 0 ? Number(compactMinAcks) : 50;
    this._compactMinIntervalMs = Number(compactMinIntervalMs) >= 0 ? Number(compactMinIntervalMs) : 60_000;
    this._lastCompactAtMs = 0;

    this._durability = durability === 'strict' ? 'strict' : 'balanced';
    this._flushIntervalMs = Number(flushIntervalMs) > 0 ? Number(flushIntervalMs) : 2000;

    this._handle = null;
    this._data = [];
    this._dirty = false;
    this._acksSinceCompact = 0;

    this._op = Promise.resolve();
    this._flushTimer = null;
  }

  async init() {
    await fsp.mkdir(this._dir, { recursive: true });

    let contents = null;
    try {
      contents = await fsp.readFile(this._filePath);
    } catch (err) {
      if (err && err.code !== 'ENOENT') throw err;
    }

    if (contents && contents.length > 0) {
      this._replay(contents);
    }

    if (this._maxSize > 0 && this._data.length > this._maxSize) {
      if (this._trimStrategy === 'oldest') {
        this._data.splice(0, this._data.length - this._maxSize);
      } else {
        this._data.splice(this._maxSize);
      }
      await this._rewriteFromData();
    }

    if (!this._handle) {
      this._handle = await fsp.open(this._filePath, 'a');
    }

    if (this._durability === 'balanced' && this._flushIntervalMs > 0) {
      this._flushTimer = setInterval(() => {
        this.flush().catch(() => undefined);
      }, this._flushIntervalMs);
      if (typeof this._flushTimer.unref === 'function') this._flushTimer.unref();
    }
  }

  length() {
    return this._data.length;
  }

  peek() {
    return this._data[0];
  }

  async enqueue(msg) {
    return this._runExclusive(async () => {
      if (msg && (msg.req || msg.res)) {
        throw new Error('Refusing to enqueue message containing req/res');
      }

      let payload;
      try {
        payload = v8.serialize(msg);
      } catch (err) {
        throw new Error(`Refusing to enqueue unserializable message: ${err && err.message ? err.message : String(err)}`);
      }

      const record = this._buildRecord(TYPE_ENQ, payload);
      await this._write(record);
      this._data.push(msg);
    });
  }

  async ack() {
    return this._runExclusive(async () => {
      if (this._data.length === 0) return;
      const record = this._buildRecord(TYPE_ACK, null);
      await this._write(record);
      this._data.shift();
      this._acksSinceCompact += 1;
      await this._compactIfNeeded();
    });
  }

  async dropOldest() {
    return this.ack();
  }

  async dropSecondOldest() {
    return this._runExclusive(async () => {
      if (this._data.length <= 1) return;
      const record = this._buildRecord(TYPE_DROP_SECOND, null);
      await this._write(record);
      this._data.splice(1, 1);
    });
  }

  async clear() {
    return this._runExclusive(async () => {
      this._data = [];
      this._acksSinceCompact = 0;
      this._lastCompactAtMs = 0;
      await this._rewriteFromData();
    });
  }

  async flush() {
    return this._runExclusive(async () => {
      if (!this._handle) return;
      if (!this._dirty) return;
      await fileDataSync(this._handle);
      this._dirty = false;
    });
  }

  async close() {
    await this._runExclusive(async () => {
      if (this._flushTimer) {
        clearInterval(this._flushTimer);
        this._flushTimer = null;
      }

      if (this._handle) {
        try {
          if (this._dirty) {
            await fileDataSync(this._handle);
            this._dirty = false;
          }
        } finally {
          await this._handle.close();
          this._handle = null;
        }
      }
    });
  }

  _runExclusive(fn) {
    this._op = this._op.then(fn, fn);
    return this._op;
  }

  _buildRecord(type, payload) {
    const payloadLen = payload ? payload.length : 0;
    const totalLen = 4 + 1 + 4 + payloadLen + 4;

    const buf = Buffer.allocUnsafe(totalLen);
    buf.writeUInt32BE(MAGIC, 0);
    buf.writeUInt8(type, 4);
    buf.writeUInt32BE(payloadLen, 5);

    if (payloadLen > 0) {
      payload.copy(buf, 9);
    }

    let crc = crc32Init();
    crc = crc32Update(crc, buf.subarray(4, 9 + payloadLen));
    crc = crc32Final(crc);
    buf.writeUInt32BE(crc, 9 + payloadLen);

    return buf;
  }

  async _write(buf) {
    if (!this._handle) throw new Error('JournalStore not initialized');

    await this._handle.write(buf, 0, buf.length, null);

    if (this._durability === 'strict') {
      await fileDataSync(this._handle);
      this._dirty = false;
    } else {
      this._dirty = true;
    }
  }

  _replay(contents) {
    let offset = 0;

    while (offset + 13 <= contents.length) {
      const magic = contents.readUInt32BE(offset);
      if (magic !== MAGIC) break;

      const type = contents.readUInt8(offset + 4);
      const payloadLen = contents.readUInt32BE(offset + 5);
      const end = offset + 9 + payloadLen + 4;
      if (end > contents.length) break;

      const crcExpected = contents.readUInt32BE(offset + 9 + payloadLen);

      let crc = crc32Init();
      crc = crc32Update(crc, contents.subarray(offset + 4, offset + 9 + payloadLen));
      crc = crc32Final(crc);

      if (crc !== crcExpected) break;

      if (type === TYPE_ENQ) {
        const payload = contents.subarray(offset + 9, offset + 9 + payloadLen);
        let msg;
        try {
          msg = v8.deserialize(payload);
        } catch {
          break;
        }
        this._data.push(msg);
      } else if (type === TYPE_ACK) {
        if (this._data.length > 0) this._data.shift();
      } else if (type === TYPE_DROP_SECOND) {
        if (this._data.length > 1) this._data.splice(1, 1);
      } else {
        break;
      }

      offset = end;
    }
  }

  async _compactIfNeeded() {
    if (this._acksSinceCompact === 0) {
      return;
    }

    if (this._maxLogSizeBytes > 0) {
      let stat;
      try {
        stat = await fsp.stat(this._filePath);
      } catch {
        stat = null;
      }

      if (stat && stat.size >= this._maxLogSizeBytes) {
        const now = Date.now();
        const timeOk = this._lastCompactAtMs === 0 || this._compactMinIntervalMs === 0 || (now - this._lastCompactAtMs) >= this._compactMinIntervalMs;
        const acksOk = this._acksSinceCompact >= this._compactMinAcks;
        if (acksOk && timeOk) {
          await this._rewriteFromData();
          this._acksSinceCompact = 0;
          this._lastCompactAtMs = now;
          return;
        }
      }
    }

    if (this._acksSinceCompact < 1000) {
      return;
    }

    let stat;
    try {
      stat = await fsp.stat(this._filePath);
    } catch {
      return;
    }

    if (!stat || stat.size < 1024 * 1024) {
      this._acksSinceCompact = 0;
      return;
    }

    await this._rewriteFromData();
    this._acksSinceCompact = 0;
  }

  async _rewriteFromData() {
    const tmpPath = path.join(this._dir, 'queue.log.tmp');
    const tmpHandle = await fsp.open(tmpPath, 'w');

    try {
      for (const msg of this._data) {
        const payload = v8.serialize(msg);
        const record = this._buildRecord(TYPE_ENQ, payload);
        await tmpHandle.write(record, 0, record.length, null);
      }

      await fileDataSync(tmpHandle);
    } finally {
      await tmpHandle.close();
    }

    await fsp.rename(tmpPath, this._filePath);

    if (this._handle) {
      await this._handle.close();
    }

    this._handle = await fsp.open(this._filePath, 'a');
    this._dirty = false;

    if (this._durability === 'strict') {
      await fileDataSync(this._handle);
    }
  }
}

module.exports = {
  JournalStore,
};
