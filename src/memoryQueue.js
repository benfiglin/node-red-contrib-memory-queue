class PubSub {
  constructor() {
    this.subscribers = []
  }

  subscribe(subscriber) {
    this.subscribers.push(subscriber)
  }

  unsubscribe(subscriber) {
    this.subscribers = this.subscribers.filter(sub => sub !== subscriber)
  }

  publish(msg) {
    this.subscribers.forEach(subscriber => subscriber(msg))
  }
}

module.exports = function(RED) {

  const path = require('path');
  const { JournalStore } = require('./journalStore');

  // configuration
  function MemoryQueueConfig(n) {
      RED.nodes.createNode(this, n);
      this.name = n.name;
      this.size = Number(n.size) > 0 ? Number(n.size) : 0;
      this.discardOnFull = !!n.discard;
      this.onFull = n.onFull || (this.discardOnFull ? 'discard_oldest' : 'reject');
      this.emitOnStart = (typeof n.emitOnStart === 'boolean') ? n.emitOnStart : true;
      this.persistent = !!n.persistent;
      this.dir = n.dir;
      this.durability = n.durability || 'balanced';
      this.flushIntervalMs = Number(n.flushIntervalMs) > 0 ? Number(n.flushIntervalMs) : 2000;
      this.onPush = new PubSub();
      this.onStatusChange = new PubSub();
      this._data = [];
      this._locked = false;

      const node = this;
      const userDir = (RED.settings && RED.settings.userDir) ? RED.settings.userDir : process.cwd();
      const baseDir = (node.dir && String(node.dir).trim().length > 0) ? node.dir : path.join(userDir, 'memory-queue');
      const trimStrategy = node.onFull === 'discard_oldest' ? 'oldest' : 'newest';

      node._store = null;
      node._ready = Promise.resolve();

      if (node.persistent) {
        node._store = new JournalStore({
          baseDir,
          key: node.id,
          maxSize: node.size,
          trimStrategy,
          durability: node.durability,
          flushIntervalMs: node.flushIntervalMs,
          maxLogSizeBytes: 10 * 1024 * 1024,
          compactMinAcks: 30,
          compactMinIntervalMs: 60_000,
        });

        node._ready = node._store.init().then(() => {
          node.onStatusChange.publish(node._store.length());
        }).catch(err => {
          node.error(err);
          throw err;
        });
      }

      node._length = function () {
        return node._store ? node._store.length() : node._data.length;
      }

      node._peek = function () {
        return node._store ? node._store.peek() : node._data[0];
      }

      node.push = async function (value) {
        await node._ready;

        const msg = RED.util.cloneMessage(value);

        const currentLength = node._length();
        const isFull = node.size > 0 && currentLength >= node.size;
        const wasLocked = node._locked;

        if (isFull) {
          if (node.onFull === 'discard_oldest') {
            if (wasLocked) {
              if (node.size <= 1) {
                node.onStatusChange.publish(node._length());
                return { ok: false, action: 'rejected' };
              }

              if (node._store) {
                await node._store.dropSecondOldest();
              } else {
                if (node._data.length > 1) node._data.splice(1, 1);
              }
            } else {
              if (node._store) {
                await node._store.dropOldest();
              } else {
                node._data.shift();
              }
            }
          } else if (node.onFull === 'discard_newest') {
            node.onStatusChange.publish(node._length());
            return { ok: true, action: 'discarded_newest' };
          } else {
            node.onStatusChange.publish(node._length());
            return { ok: false, action: 'rejected' };
          }
        }

        if (node.size > 0 && node._length() >= node.size) {
          node.onStatusChange.publish(node._length());
          return { ok: false, action: 'rejected' };
        }

        if (node._store) {
          await node._store.enqueue(msg);
        } else {
          node._data.push(msg);
        }

        node.onStatusChange.publish(node._length());

        if (node._length() === 1 && !node._locked) {
          await node.emitNext(false);
        }

        return { ok: true, action: 'enqueued' };
      }

      node.emitNext = async function (acked = false) {
        await node._ready;

        if (acked) {
          if (node._store) {
            await node._store.ack();
          } else {
            node._data.shift();
          }
        }

        node._locked = false;
        if (node._length() > 0) {
          node._locked = true;
          node.onPush.publish(node._peek());
        }

        node.onStatusChange.publish(node._length());
      }

      node.clear = async function () {
        await node._ready;

        node._locked = false;
        if (node._store) {
          await node._store.clear();
        } else {
          node._data = [];
        }

        node.onStatusChange.publish(node._length());
      }

      node.on('close', function(removed, done) {
        const _done = (typeof done === 'function') ? done : ((typeof removed === 'function') ? removed : null);
        const closePromise = (async () => {
          try {
            await node._ready;
          } catch {
            return;
          }
          if (node._store) {
            await node._store.close();
          }
        })();

        closePromise.finally(() => {
          try { if (_done) _done(); } catch {}
        });
      });
  }
  RED.nodes.registerType("memory-queue", MemoryQueueConfig);

  // queue in
  function QueueInNode(config) {
    RED.nodes.createNode(this, config);
    const queue = RED.nodes.getNode(config.queue);
    const node = this;

    if (!queue) {
      node.status({fill: "red", shape:"ring", text:"missing queue"});
      return;
    }
    
    node.on('input', async function(msg, send, done) {
      try {
        const result = await queue.push(msg);
        if (result.ok && result.action === 'enqueued') {
          node.status({fill: "green", shape:"dot", text:"msg enqueued"});
        } else if (result.ok && result.action === 'discarded_newest') {
          node.status({fill: "yellow", shape:"ring", text:"msg discarded"});
        } else {
          node.status({fill: "red", shape:"ring", text:"queue full"});
        }
        (send || node.send).call(node, msg);
        if (typeof done === 'function') done();
      } catch (err) {
        node.status({fill: "red", shape:"ring", text:"enqueue error"});
        if (typeof done === 'function') done(err);
      }
    });
  }
  RED.nodes.registerType("memqueue in", QueueInNode);

  // queue out
  function QueueOutNode(config) {
    RED.nodes.createNode(this, config);
    const queue = RED.nodes.getNode(config.queue);
    const node = this;

    if (!queue) {
      node.status({fill: "red", shape:"ring", text:"missing queue"});
      return;
    }

    const emitMessage = function (msg) {
      node.send(RED.util.cloneMessage(msg));
    }

    const updateStatus = function (length) {
      node.status({fill: length > 0 ? "yellow" : "green", shape:"dot", text:`${length} in queue`});
    }
    updateStatus(0);

    queue.onPush.subscribe(emitMessage);
    queue.onStatusChange.subscribe(updateStatus);

    if (queue.emitOnStart) {
      queue.emitNext(false).catch(() => undefined);
    }

    this.on('close', function(done) {
      queue.onPush.unsubscribe(emitMessage);
      queue.onStatusChange.unsubscribe(updateStatus);
      done();
    });
  }
  RED.nodes.registerType("memqueue out", QueueOutNode);

  // queue ack
  function QueueAckNode(config) {
    RED.nodes.createNode(this, config);
    const queue = RED.nodes.getNode(config.queue);
    const node = this;

    if (!queue) {
      node.status({fill: "red", shape:"ring", text:"missing queue"});
      return;
    }

    node.on('input', async function(msg, send, done) {
      try {
        await queue.emitNext(true);
        if (typeof done === 'function') done();
      } catch (err) {
        if (typeof done === 'function') done(err);
      }
    });
  }
  RED.nodes.registerType("memqueue ack", QueueAckNode);

  // queue resend
  function QueueResendNode(config) {
    RED.nodes.createNode(this, config);
    const queue = RED.nodes.getNode(config.queue);
    const node = this;

    if (!queue) {
      node.status({fill: "red", shape:"ring", text:"missing queue"});
      return;
    }

    node.on('input', async function(msg, send, done) {
      try {
        await queue.emitNext(false);
        if (typeof done === 'function') done();
      } catch (err) {
        if (typeof done === 'function') done(err);
      }
    });
  }
  RED.nodes.registerType("memqueue resend", QueueResendNode);

  // queue clear
  function QueueClearNode(config) {
    RED.nodes.createNode(this, config);
    const queue = RED.nodes.getNode(config.queue);
    const node = this;

    if (!queue) {
      node.status({fill: "red", shape:"ring", text:"missing queue"});
      return;
    }

    node.on('input', async function(msg, send, done) {
      try {
        await queue.clear();
        if (typeof done === 'function') done();
      } catch (err) {
        if (typeof done === 'function') done(err);
      }
    });
  }
  RED.nodes.registerType("memqueue clear", QueueClearNode);
}
