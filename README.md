# node-red-contrib-better-queue

Forked from `node-red-contrib-memory-queue` (https://github.com/Capelinha/node-red-contrib-memory-queue).

This node was designed to control a queue in memory or on disk.

<!-- Install
-----------
You can install this node directly from the "Manage Palette" menu in the Node-RED interface. There are no compilation steps.

```
npm install node-red-contrib-memory-queue
``` -->

<!-- How to use
-----------
![Example Flow](/example/flow_example.png)

### Configure the queue config
Configure the queue configuration with a unique name for the queue. To create multiple queues it is necessary to create new configurations with different names.

Optionally, it is possible to limit the queue size, and to choose the desired behavior when the queue is full (whether to ignore the incoming messages or discard the oldest message in the queue and replace it with the incoming message).

![Example Configure](/example/config_example.png) -->

Changelog
-----------
### 4.0.0
New persistent storage feature.

* Forked from `node-red-contrib-memory-queue` (https://github.com/Capelinha/node-red-contrib-memory-queue).
* Added `persistent` option to enable **persistent storage** of the queue.
* Added `dir` option to specify the directory where the queue files are stored.
* Added `durability` option to specify the durability of the queue.
* Added `flushIntervalMs` option to specify the interval at which the queue is flushed to disk.
* Added `emitOnStart` option to emit the head of the queue on start.

### 3.0.0
* Add the ability to limit the queue size.
* Add an option to discard the oldest message in the queue when full and replace it with the incoming message.
* Add a new `queue resend` node type to output the same message from the queue again (instead of Acking).
* Add status messages to the `queue in` and the `queue out` nodes.
* Fix potential immutability issues through a deep clone of each message on input and output

### 2.0.0
* The entire msg object is added to the queue, not just the payload
