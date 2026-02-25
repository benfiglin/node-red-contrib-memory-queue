const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const v8 = require('node:v8');

function cloneMessage(msg) {
  // Close enough for tests; preserves Buffers/Dates/etc.
  return v8.deserialize(v8.serialize(msg));
}

function makeRED() {
  const registered = {};

  const RED = {
    settings: {
      userDir: '/tmp',
    },
    util: {
      cloneMessage,
    },
    nodes: {
      registerType(name, ctor) {
        registered[name] = ctor;
      },
      createNode(node, n) {
        EventEmitter.call(node);
        Object.setPrototypeOf(node, EventEmitter.prototype);

        node.id = n && n.id ? n.id : 'test-id';
        node.status = function () {};
        node.send = function () {};
        node.warn = function () {};
        node.error = function () {};
      },
      getNode() {
        return null;
      },
    },
  };

  return { RED, registered };
}

function capturePublishes(pubsub) {
  const published = [];
  pubsub.subscribe((msg) => published.push(msg));
  return published;
}

test('MemoryQueueConfig (in-memory): emits first message and locks until ack', async () => {
  const { RED, registered } = makeRED();
  require('../src/memoryQueue')(RED);

  const MemoryQueue = registered['memory-queue'];
  assert.equal(typeof MemoryQueue, 'function');

  const q = new MemoryQueue({ name: 'q', size: 10, discard: false, onFull: 'reject' });
  const pushed = capturePublishes(q.onPush);

  const r1 = await q.push({ n: 1 });
  assert.equal(r1.ok, true);
  assert.equal(q._locked, true);
  assert.equal(q._length(), 1);
  assert.deepEqual(pushed[0], { n: 1, _memqueue: { resendCount: 0 } });

  // pushing second should not emit because still locked
  await q.push({ n: 2 });
  assert.equal(pushed.length, 1);

  await q.emitNext(true); // ack
  assert.equal(q._length(), 1);
  assert.equal(q._locked, true);
  assert.deepEqual(pushed[1], { n: 2, _memqueue: { resendCount: 0 } });
});

test('MemoryQueueConfig (in-memory): reject when full', async () => {
  const { RED, registered } = makeRED();
  require('../src/memoryQueue')(RED);
  const MemoryQueue = registered['memory-queue'];

  const q = new MemoryQueue({ name: 'q', size: 1, discard: false, onFull: 'reject' });
  capturePublishes(q.onPush);

  const r1 = await q.push({ n: 1 });
  assert.equal(r1.ok, true);

  const r2 = await q.push({ n: 2 });
  assert.equal(r2.ok, false);
  assert.equal(r2.action, 'rejected');
  assert.equal(q._length(), 1);
});

test('MemoryQueueConfig (in-memory): discard_newest when full (silently drops incoming)', async () => {
  const { RED, registered } = makeRED();
  require('../src/memoryQueue')(RED);
  const MemoryQueue = registered['memory-queue'];

  const q = new MemoryQueue({ name: 'q', size: 1, discard: false, onFull: 'discard_newest' });
  const pushed = capturePublishes(q.onPush);

  await q.push({ n: 1 });
  const r2 = await q.push({ n: 2 });

  assert.equal(r2.ok, true);
  assert.equal(r2.action, 'discarded_newest');
  assert.equal(q._length(), 1);
  assert.deepEqual(pushed[0], { n: 1, _memqueue: { resendCount: 0 } });
});

test('MemoryQueueConfig (in-memory): discard_oldest while locked does not drop in-flight head', async () => {
  const { RED, registered } = makeRED();
  require('../src/memoryQueue')(RED);
  const MemoryQueue = registered['memory-queue'];

  const q = new MemoryQueue({ name: 'q', size: 2, discard: true, onFull: 'discard_oldest' });
  const pushed = capturePublishes(q.onPush);

  await q.push({ n: 1 });
  await q.push({ n: 2 });

  // Currently locked with head=1, queue=[1,2]
  assert.equal(q._locked, true);
  assert.deepEqual(pushed[0], { n: 1, _memqueue: { resendCount: 0 } });

  // Push 3 when full: should drop oldest (1). Because it was locked,
  // it should re-emit current head (2) after dropping.
  await q.push({ n: 3 });

  assert.equal(q._length(), 2);
  // New behavior: do not drop in-flight head (1) and do not re-emit while locked.
  // Instead drop the next-oldest (2), so after enqueue it should be [1,3].
  assert.equal(pushed.length, 1);
  assert.deepEqual(q._peek(), { n: 1 });

  await q.emitNext(true); // ack 1
  assert.deepEqual(pushed[1], { n: 3, _memqueue: { resendCount: 0 } });
  assert.deepEqual(q._peek(), { n: 3 });
});

test('MemoryQueueConfig (in-memory): resend increments resendCount on the in-flight head', async () => {
  const { RED, registered } = makeRED();
  require('../src/memoryQueue')(RED);
  const MemoryQueue = registered['memory-queue'];

  const q = new MemoryQueue({ name: 'q', size: 10, discard: false, onFull: 'reject' });
  const pushed = capturePublishes(q.onPush);

  await q.push({ n: 1 });
  await q.push({ n: 2 });

  await q.emitNext(false, { resend: true });
  assert.deepEqual(pushed[1], { n: 1, _memqueue: { resendCount: 1 } });

  await q.emitNext(false, { resend: true });
  assert.deepEqual(pushed[2], { n: 1, _memqueue: { resendCount: 2 } });

  await q.emitNext(true); // ack 1, move to 2
  assert.deepEqual(pushed[3], { n: 2, _memqueue: { resendCount: 0 } });
});

test('MemoryQueueConfig (in-memory): discard_oldest when size=1 and locked rejects', async () => {
  const { RED, registered } = makeRED();
  require('../src/memoryQueue')(RED);
  const MemoryQueue = registered['memory-queue'];

  const q = new MemoryQueue({ name: 'q', size: 1, discard: true, onFull: 'discard_oldest' });
  capturePublishes(q.onPush);

  await q.push({ n: 1 });
  assert.equal(q._locked, true);

  const r2 = await q.push({ n: 2 });
  assert.equal(r2.ok, false);
  assert.equal(r2.action, 'rejected');
  assert.equal(q._length(), 1);
  assert.deepEqual(q._peek(), { n: 1 });
});

test('memqueue out: emits head on start when enabled', async () => {
  const registered = {};

  const RED = {
    settings: {
      userDir: '/tmp',
    },
    util: {
      cloneMessage,
    },
    nodes: {
      registerType(name, ctor) {
        registered[name] = ctor;
      },
      createNode(node, n) {
        EventEmitter.call(node);
        Object.setPrototypeOf(node, EventEmitter.prototype);

        node.id = n && n.id ? n.id : 'test-id';
        node.status = function () {};
        node.send = function () {};
        node.warn = function () {};
        node.error = function () {};
      },
      getNode(id) {
        return id === 'q1' ? this._queue : null;
      },
      _queue: null,
    },
  };

  require('../src/memoryQueue')(RED);
  const MemoryQueue = registered['memory-queue'];
  const QueueOut = registered['memqueue out'];

  const q = new MemoryQueue({ id: 'q1', name: 'q', size: 10, discard: false, onFull: 'reject', emitOnStart: true });
  let calls = 0;
  q.emitNext = async function () { calls += 1; };
  RED.nodes._queue = q;

  new QueueOut({ queue: 'q1', name: 'out' });
  assert.equal(calls, 1);
});

test('memqueue out: does not emit head on start when disabled', async () => {
  const registered = {};

  const RED = {
    settings: {
      userDir: '/tmp',
    },
    util: {
      cloneMessage,
    },
    nodes: {
      registerType(name, ctor) {
        registered[name] = ctor;
      },
      createNode(node, n) {
        EventEmitter.call(node);
        Object.setPrototypeOf(node, EventEmitter.prototype);

        node.id = n && n.id ? n.id : 'test-id';
        node.status = function () {};
        node.send = function () {};
        node.warn = function () {};
        node.error = function () {};
      },
      getNode(id) {
        return id === 'q1' ? this._queue : null;
      },
      _queue: null,
    },
  };

  require('../src/memoryQueue')(RED);
  const MemoryQueue = registered['memory-queue'];
  const QueueOut = registered['memqueue out'];

  const q = new MemoryQueue({ id: 'q1', name: 'q', size: 10, discard: false, onFull: 'reject', emitOnStart: false });
  let calls = 0;
  q.emitNext = async function () { calls += 1; };
  RED.nodes._queue = q;

  new QueueOut({ queue: 'q1', name: 'out' });
  assert.equal(calls, 0);
});

test('MemoryQueueConfig (in-memory): clear empties and unlocks', async () => {
  const { RED, registered } = makeRED();
  require('../src/memoryQueue')(RED);
  const MemoryQueue = registered['memory-queue'];

  const q = new MemoryQueue({ name: 'q', size: 10, discard: false, onFull: 'reject' });
  await q.push({ n: 1 });
  await q.push({ n: 2 });
  assert.equal(q._length(), 2);
  assert.equal(q._locked, true);

  await q.clear();
  assert.equal(q._length(), 0);
  assert.equal(q._locked, false);
  assert.equal(q._peek(), undefined);
});
