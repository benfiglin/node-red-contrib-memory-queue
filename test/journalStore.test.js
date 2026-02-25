const test = require('node:test');
const assert = require('node:assert/strict');
const os = require('node:os');
const path = require('node:path');
const fsp = require('node:fs/promises');

const { JournalStore } = require('../src/journalStore');

async function withTempDir(fn) {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'memqueue-test-'));
  try {
    return await fn(dir);
  } finally {
    await fsp.rm(dir, { recursive: true, force: true });
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

test('JournalStore: enqueue/ack survives restart (replay)', async () => {
  await withTempDir(async (baseDir) => {
    const key = 'q1';

    const s1 = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await s1.init();
    await s1.enqueue({ a: 1 });
    await s1.enqueue({ a: 2 });
    await s1.enqueue({ a: 3 });

    assert.equal(s1.length(), 3);
    assert.deepEqual(s1.peek(), { a: 1 });

    await s1.ack();
    assert.equal(s1.length(), 2);
    assert.deepEqual(s1.peek(), { a: 2 });

    await s1.close();

    const s2 = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await s2.init();
    assert.equal(s2.length(), 2);
    assert.deepEqual(s2.peek(), { a: 2 });
    await s2.close();
  });
});

test('JournalStore: dropSecondOldest removes index 1 and persists across restart', async () => {
  await withTempDir(async (baseDir) => {
    const key = 'q13';

    const s1 = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await s1.init();
    await s1.enqueue({ n: 1 });
    await s1.enqueue({ n: 2 });
    await s1.enqueue({ n: 3 });
    assert.deepEqual(s1.peek(), { n: 1 });
    assert.equal(s1.length(), 3);

    await s1.dropSecondOldest();
    assert.equal(s1.length(), 2);
    assert.deepEqual(s1.peek(), { n: 1 });
    await s1.ack();
    assert.deepEqual(s1.peek(), { n: 3 });
    await s1.close();

    const s2 = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await s2.init();
    assert.equal(s2.length(), 1);
    assert.deepEqual(s2.peek(), { n: 3 });
    await s2.close();
  });
});

test('JournalStore: truncation at end of log is tolerated (corruption resistance)', async () => {
  await withTempDir(async (baseDir) => {
    const key = 'q2';

    const s1 = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await s1.init();
    await s1.enqueue({ n: 1 });
    await s1.enqueue({ n: 2 });
    await s1.close();

    const logPath = path.join(baseDir, `memqueue-${key}`, 'queue.log');
    const buf = await fsp.readFile(logPath);

    // Truncate some bytes from the end so the final record becomes incomplete.
    await fsp.writeFile(logPath, buf.subarray(0, Math.max(0, buf.length - 7)));

    const s2 = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await s2.init();

    // Should replay at least the first message. The second may be dropped due to truncation.
    assert.equal(s2.length(), 1);
    assert.deepEqual(s2.peek(), { n: 1 });

    await s2.close();
  });
});

test('JournalStore: trims on startup to maxSize (oldest keeps newest, newest keeps oldest)', async () => {
  await withTempDir(async (baseDir) => {
    const keyKeepNewest = 'q3-keep-newest';
    const keyKeepOldest = 'q3-keep-oldest';

    const seed = new JournalStore({
      baseDir,
      key: keyKeepNewest,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await seed.init();
    await seed.enqueue({ n: 1 });
    await seed.enqueue({ n: 2 });
    await seed.enqueue({ n: 3 });
    await seed.close();

    const keepNewest2 = new JournalStore({
      baseDir,
      key: keyKeepNewest,
      maxSize: 2,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await keepNewest2.init();
    assert.equal(keepNewest2.length(), 2);
    assert.deepEqual(keepNewest2.peek(), { n: 2 });
    await keepNewest2.close();

    // Seed a separate queue for the keep-oldest scenario
    const seed2 = new JournalStore({
      baseDir,
      key: keyKeepOldest,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await seed2.init();
    await seed2.enqueue({ n: 1 });
    await seed2.enqueue({ n: 2 });
    await seed2.enqueue({ n: 3 });
    await seed2.close();

    const keepOldest2 = new JournalStore({
      baseDir,
      key: keyKeepOldest,
      maxSize: 2,
      trimStrategy: 'newest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await keepOldest2.init();
    assert.equal(keepOldest2.length(), 2);
    assert.deepEqual(keepOldest2.peek(), { n: 1 });
    await keepOldest2.close();
  });
});

test('JournalStore: refuses req/res and unserializable payloads', async () => {
  await withTempDir(async (baseDir) => {
    const key = 'q4';

    const s = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await s.init();

    await assert.rejects(() => s.enqueue({ req: {} }), /Refusing to enqueue message containing req\/res/);
    await assert.rejects(() => s.enqueue({ res: {} }), /Refusing to enqueue message containing req\/res/);
    await assert.rejects(() => s.enqueue({ fn: () => undefined }), /unserializable/i);

    await s.close();
  });
});

test('JournalStore: clear empties and persists across restart', async () => {
  await withTempDir(async (baseDir) => {
    const key = 'q12';

    const s1 = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await s1.init();
    await s1.enqueue({ a: 1 });
    await s1.enqueue({ a: 2 });
    assert.equal(s1.length(), 2);

    await s1.clear();
    assert.equal(s1.length(), 0);
    await s1.close();

    const s2 = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await s2.init();
    assert.equal(s2.length(), 0);
    await s2.close();
  });
});

test('JournalStore: size-based compaction is debounced (does not compact on every ack)', async () => {
  await withTempDir(async (baseDir) => {
    const key = 'q11';

    const s = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
      maxLogSizeBytes: 200,
      compactMinAcks: 5,
      compactMinIntervalMs: 60_000,
    });

    await s.init();

    // Write a few messages so the log becomes larger than maxLogSizeBytes.
    for (let i = 0; i < 8; i++) {
      await s.enqueue({ n: i, payload: 'x'.repeat(50) });
    }

    const logPath = path.join(baseDir, `memqueue-${key}`, 'queue.log');
    const stBeforeAck = await fsp.stat(logPath);
    assert.ok(stBeforeAck.size > 200);

    // Acking a few times should NOT compact yet because compactMinAcks is 5.
    for (let i = 0; i < 4; i++) {
      await s.ack();
    }
    const stAfter4 = await fsp.stat(logPath);
    // Size will usually increase slightly because ACK records are appended.
    // The key property we want is: no rewrite compaction yet (which would drop size significantly).
    assert.ok(stAfter4.size >= stBeforeAck.size);

    // The 5th ack crosses the threshold, so compaction may happen.
    await s.ack();
    const stAfter5 = await fsp.stat(logPath);
    assert.ok(stAfter5.size <= stBeforeAck.size);

    await s.close();
  });
});

test('JournalStore (balanced): append is visible immediately but datasync is deferred until flush/close', async () => {
  await withTempDir(async (baseDir) => {
    const key = 'q7';

    const s = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'balanced',
      flushIntervalMs: 10_000,
    });

    await s.init();

    let datasyncCount = 0;
    const originalDatasync = s._handle.datasync.bind(s._handle);
    s._handle.datasync = async function () {
      datasyncCount += 1;
      return originalDatasync();
    };

    await s.enqueue({ n: 1 });

    const logPath = path.join(baseDir, `memqueue-${key}`, 'queue.log');
    const st1 = await fsp.stat(logPath);
    assert.ok(st1.size > 0);

    // No fsync/datasync should have happened yet in balanced mode.
    assert.equal(datasyncCount, 0);

    await s.flush();
    assert.equal(datasyncCount, 1);

    await s.close();
  });
});

test('JournalStore (strict): datasync happens on each enqueue/ack', async () => {
  await withTempDir(async (baseDir) => {
    const key = 'q8';

    const s = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 10_000,
    });

    await s.init();

    let datasyncCount = 0;
    const originalDatasync = s._handle.datasync.bind(s._handle);
    s._handle.datasync = async function () {
      datasyncCount += 1;
      return originalDatasync();
    };

    await s.enqueue({ n: 1 });
    await s.enqueue({ n: 2 });
    await s.ack();

    assert.ok(datasyncCount >= 3);
    await s.close();
  });
});

test('JournalStore (balanced): periodic flush timer triggers datasync', async () => {
  await withTempDir(async (baseDir) => {
    const key = 'q9';

    const s = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'balanced',
      flushIntervalMs: 25,
    });

    await s.init();

    let datasyncCount = 0;
    const originalDatasync = s._handle.datasync.bind(s._handle);
    s._handle.datasync = async function () {
      datasyncCount += 1;
      return originalDatasync();
    };

    await s.enqueue({ n: 1 });

    await sleep(80);
    assert.ok(datasyncCount >= 1);

    await s.close();
  });
});

test('JournalStore: CRC corruption stops replay at first bad record', async () => {
  await withTempDir(async (baseDir) => {
    const key = 'q5';

    const s1 = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await s1.init();
    await s1.enqueue({ n: 1 });
    await s1.enqueue({ n: 2 });
    await s1.enqueue({ n: 3 });
    await s1.close();

    const logPath = path.join(baseDir, `memqueue-${key}`, 'queue.log');
    const buf = await fsp.readFile(logPath);

    // Flip a byte somewhere after the first record header to break CRC of a later record.
    const corrupted = Buffer.from(buf);
    const idx = Math.min(corrupted.length - 1, 32);
    corrupted[idx] = corrupted[idx] ^ 0xff;
    await fsp.writeFile(logPath, corrupted);

    const s2 = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await s2.init();
    assert.ok(s2.length() >= 1);
    assert.deepEqual(s2.peek(), { n: 1 });
    await s2.close();
  });
});

test('JournalStore: ack on empty is a no-op', async () => {
  await withTempDir(async (baseDir) => {
    const key = 'q6';

    const s = new JournalStore({
      baseDir,
      key,
      maxSize: 0,
      trimStrategy: 'oldest',
      durability: 'strict',
      flushIntervalMs: 1,
    });

    await s.init();
    assert.equal(s.length(), 0);
    await s.ack();
    assert.equal(s.length(), 0);
    await s.close();
  });
});
