// Run with: node --test tests/frankenTimeout.test.mjs
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mkdtempSync } from 'node:fs';
import { statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { createConnection } from 'node:net';
import { join } from 'node:path';
import { PassThrough } from 'node:stream';
import { test } from 'node:test';

const dataFolder = mkdtempSync(join(tmpdir(), 'free-sleep-franken-'));
process.env.DATA_FOLDER = dataFolder;
process.env.ENV = 'local';

const { MessageReadTimeoutError, MessageStream, MessageStreamInvalidatedError } = await import(
  '../server/src/8sleep/messageStream.ts'
);
const {
  FrankenConnection,
  FrankenConnectCancelledError,
  FrankenSocketClosedError,
  callOnFranken,
} = await import('../server/src/8sleep/frankenServer.ts');

const fastTiming = {
  slowReplyWarnMs: 30,
  responseDeadlineMs: 90,
  responseDelayMs: 0,
  connectionTimeoutMs: 400,
  connectionMaxAttempts: 4,
};

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function waitFor(predicate, timeoutMs = 1000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (await predicate()) return;
    await delay(5);
  }
  throw new Error('timed out waiting for condition');
}

function connectPeer(sockPath) {
  const socket = createConnection(sockPath);
  socket.on('error', () => {});
  return new Promise((resolve, reject) => {
    socket.once('connect', () => resolve(socket));
    socket.once('error', reject);
  });
}

class FramePeer {
  constructor(socket, onRequest) {
    this.socket = socket;
    this.onRequest = onRequest;
    this.buffer = Buffer.alloc(0);
    this.requests = [];
    socket.on('data', (chunk) => this.onData(chunk));
  }

  onData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    const separator = Buffer.from('\n\n');
    let splitAt = this.buffer.indexOf(separator);
    while (splitAt >= 0) {
      const request = this.buffer.subarray(0, splitAt).toString();
      this.buffer = this.buffer.subarray(splitAt + separator.length);
      this.requests.push(request);
      this.onRequest(request, this);
      splitAt = this.buffer.indexOf(separator);
    }
  }

  reply(payload) {
    if (this.socket.destroyed) return;
    this.socket.write(`${payload}\n\n`);
  }
}

function fileExists(path) {
  try {
    statSync(path);
    return true;
  } catch {
    return false;
  }
}

async function listen(connection) {
  const pending = connection.connect();
  await waitFor(() => fileExists(connection.socketPath));
  // Return the promise without awaiting it. Awaiting connect() here deadlocks
  // because the client has not connected yet.
  return { pending };
}

function assertUnsent(error) {
  assert.equal(error instanceof FrankenSocketClosedError, true);
  assert.notEqual(error.code, 'ERR_STREAM_DESTROYED');
  assert.equal(String(error.message).includes('Cannot call write after a stream was destroyed'), false);
  return true;
}

test('an already framed reply is returned to the outstanding read', async () => {
  const readable = new PassThrough();
  const stream = new MessageStream(readable);
  readable.write('READY\n\n');
  await delay(10);
  const startedAt = Date.now();
  const message = await stream.readMessage({ deadlineMs: 500, warnAfterMs: 100 });
  assert.equal(message.toString(), 'READY');
  assert.ok(Date.now() - startedAt < 50);
  readable.destroy();
});

test('a reply that crosses the warning stays with the outstanding command', async () => {
  const readable = new PassThrough();
  const stream = new MessageStream(readable);
  let slowNotices = 0;
  const pending = stream.readMessage({
    deadlineMs: 200,
    warnAfterMs: 30,
    onSlow: () => {
      slowNotices += 1;
    },
  });
  await delay(45);
  readable.write('SLOW\n\n');
  assert.equal((await pending).toString(), 'SLOW');
  assert.equal(slowNotices, 1);
  readable.destroy();
});

test('a frame completed before the deadline is not discarded as a timeout', async () => {
  const readable = new PassThrough();
  const stream = new MessageStream(readable);
  const pending = stream.readMessage({ deadlineMs: 80, warnAfterMs: 20 });
  await delay(15);
  readable.write(Buffer.from('PAR'));
  await delay(15);
  readable.write(Buffer.from('TIAL\n\n'));
  assert.equal((await pending).toString(), 'PARTIAL');
  readable.destroy();
});

test('timing out a read does not leave an unhandled rejection when the peer later fails', async () => {
  const unhandled = [];
  const onUnhandled = (reason) => unhandled.push(reason);
  process.on('unhandledRejection', onUnhandled);
  const readable = new PassThrough();
  const stream = new MessageStream(readable);
  try {
    await assert.rejects(
      stream.readMessage({ deadlineMs: 30 }),
      (error) => error instanceof MessageReadTimeoutError,
    );
    readable.destroy(new Error('peer reset'));
    await delay(30);
    assert.deepEqual(unhandled, []);
  } finally {
    process.off('unhandledRejection', onUnhandled);
    readable.destroy();
  }
});

test('a late frame after invalidation cannot be read as a later reply', async () => {
  const readable = new PassThrough();
  const stream = new MessageStream(readable);
  await assert.rejects(
    stream.readMessage({ deadlineMs: 25 }),
    (error) => error instanceof MessageReadTimeoutError,
  );
  stream.invalidate();
  readable.write('LATE\n\n');
  await delay(10);
  await assert.rejects(
    stream.readMessage({ deadlineMs: 40 }),
    (error) => error instanceof MessageStreamInvalidatedError,
  );
  readable.destroy();
});

test('slow replies stay ordered and a following command is not sent early', async () => {
  const sockPath = join(dataFolder, 'ordered.sock');
  const connection = new FrankenConnection(sockPath, fastTiming);
  let peerSocket;
  try {
    const { pending } = await listen(connection);
    peerSocket = await connectPeer(sockPath);
    const received = [];
    const peer = new FramePeer(peerSocket, (request, current) => {
      received.push(request);
      const waitMs = request === '14' ? 50 : 5;
      setTimeout(() => current.reply(`R:${request}`), waitMs);
    });
    const franken = await pending;
    const first = franken.sendMessage('14');
    const second = franken.sendMessage('11\n5');
    assert.equal(await first, 'R:14');
    assert.equal(await second, 'R:11\n5');
    assert.deepEqual(received, ['14', '11\n5']);
    assert.equal(franken.isClosed, false);
    assert.equal(peer.requests.length, 2);
  } finally {
    await connection.disconnect();
    peerSocket?.destroy();
  }
});

test('a 20475ms RIGHT_TEMP_DURATION reply stays with that command', async () => {
  // Production ratio: warn 10s, observed success 20475ms, deadline 30s.
  const sockPath = join(dataFolder, 'right-duration.sock');
  const connection = new FrankenConnection(sockPath, {
    ...fastTiming,
    slowReplyWarnMs: 40,
    responseDeadlineMs: 500,
  });
  let peerSocket;
  try {
    const { pending } = await listen(connection);
    peerSocket = await connectPeer(sockPath);
    const received = [];
    const franken = await pending;
    new FramePeer(peerSocket, (request, current) => {
      received.push({ request, at: Date.now() });
      const waitMs = request.startsWith('10\n') ? 82 : 5;
      setTimeout(() => current.reply(`R:${request}`), waitMs);
    });
    const startedAt = Date.now();
    const right = franken.sendMessage('10\n43200');
    const left = franken.sendMessage('9\n43200');
    assert.equal(await right, 'R:10\n43200');
    const rightElapsed = Date.now() - startedAt;
    assert.ok(rightElapsed >= 82, `right elapsed ${rightElapsed}`);
    assert.equal(await left, 'R:9\n43200');
    assert.deepEqual(received.map((entry) => entry.request), ['10\n43200', '9\n43200']);
    assert.ok(received[1].at >= received[0].at + 70);
    assert.equal(franken.isClosed, false);
  } finally {
    await connection.disconnect();
    peerSocket?.destroy();
  }
});

test('a missing reply invalidates the socket before the queued command is written', async () => {
  const sockPath = join(dataFolder, 'missing.sock');
  const connection = new FrankenConnection(sockPath, fastTiming);
  let peerSocket;
  try {
    const { pending } = await listen(connection);
    peerSocket = await connectPeer(sockPath);
    const peer = new FramePeer(peerSocket, () => {});
    const franken = await pending;
    const inode = statSync(sockPath).ino;
    const first = franken.sendMessage('14');
    const second = franken.sendMessage('10\n43200');
    await assert.rejects(first, (error) => error instanceof MessageReadTimeoutError);
    await assert.rejects(second, assertUnsent);
    assert.deepEqual(peer.requests, ['14']);
    assert.equal(franken.isClosed, true);
    assert.equal(statSync(sockPath).ino, inode);
    assert.equal(connection.listenSocketRebinds, 0);
    if (!peerSocket.destroyed) peerSocket.write('LATE14\n\n');
    await delay(15);
  } finally {
    await connection.disconnect();
    peerSocket?.destroy();
  }
});

test('write after the client socket is destroyed does not surface ERR_STREAM_DESTROYED', async () => {
  const sockPath = join(dataFolder, 'destroyed.sock');
  const connection = new FrankenConnection(sockPath, fastTiming);
  const requests = [];
  let peerSocket;
  try {
    const { pending } = await listen(connection);
    peerSocket = await connectPeer(sockPath);
    new FramePeer(peerSocket, (request) => requests.push(request));
    const franken = await pending;
    franken.beforeWrite = () => {
      franken.beforeWrite = undefined;
      franken.close();
    };
    await assert.rejects(franken.sendMessage('10\n43200'), assertUnsent);
    assert.deepEqual(requests, []);

    const retried = callOnFranken(
      () => connection.connect(),
      (session) => session.sendMessage('10\n43200'),
    );
    const retryPeer = await connectPeer(sockPath);
    try {
      new FramePeer(retryPeer, (request, current) => {
        requests.push(request);
        current.reply(`OK:${request}`);
      });
      assert.equal(await retried, 'OK:10\n43200');
      assert.deepEqual(requests, ['10\n43200']);
      assert.equal(connection.listenSocketRebinds, 0);
    } finally {
      retryPeer.destroy();
    }
  } finally {
    await connection.disconnect();
    peerSocket?.destroy();
  }
});

test('the unsent command is retried on a new client and does not consume the late reply', async () => {
  const sockPath = join(dataFolder, 'recover.sock');
  const connection = new FrankenConnection(sockPath, fastTiming);
  const requests = [];
  const stopped = { current: false };
  const acceptLoop = (async () => {
    while (!stopped.current) {
      let socket;
      try {
        socket = await connectPeer(sockPath);
      } catch {
        await delay(5);
        continue;
      }
      const peer = new FramePeer(socket, (request, current) => {
        requests.push(request);
        if (request === '14') return;
        current.reply(`OK:${request}`);
      });
      await once(socket, 'close').catch(() => {});
      void peer;
      if (!socket.destroyed) socket.destroy();
    }
  })();

  try {
    const first = callOnFranken(() => connection.connect(), (franken) => franken.sendMessage('14'));
    await waitFor(() => requests.includes('14'));
    const inode = statSync(sockPath).ino;
    const second = callOnFranken(
      () => connection.connect(),
      (franken) => franken.sendMessage('10\n43200'),
    );
    await assert.rejects(first, (error) => error instanceof MessageReadTimeoutError);
    assert.equal(await second, 'OK:10\n43200');
    assert.deepEqual(requests, ['14', '10\n43200']);
    assert.equal(statSync(sockPath).ino, inode);
    assert.equal(connection.listenSocketRebinds, 0);
  } finally {
    stopped.current = true;
    await connection.disconnect();
    await acceptLoop;
  }
});

test('the listen socket is rebound at most once before any client, then left in place', async () => {
  const sockPath = join(dataFolder, 'rebind.sock');
  const timing = {
    ...fastTiming,
    connectionTimeoutMs: 40,
    connectionMaxAttempts: 8,
  };
  const connection = new FrankenConnection(sockPath, timing);
  const pending = connection.connect();
  await waitFor(() => connection.listenSocketRebinds === 1 && fileExists(sockPath));
  const inode = statSync(sockPath).ino;
  await delay(100);
  assert.equal(connection.listenSocketRebinds, 1);
  assert.equal(statSync(sockPath).ino, inode);
  const peerSocket = await connectPeer(sockPath);
  const franken = await pending;
  assert.equal(franken.isClosed, false);
  assert.equal(connection.listenSocketRebinds, 1);
  await connection.disconnect();
  peerSocket.destroy();
});

test('disconnect cancels a wait without requiring every connection attempt', async () => {
  const sockPath = join(dataFolder, 'cancel.sock');
  const connection = new FrankenConnection(sockPath, {
    ...fastTiming,
    connectionTimeoutMs: 50,
    connectionMaxAttempts: 10,
  });
  const pending = connection.connect();
  await waitFor(() => fileExists(sockPath));
  await connection.disconnect();
  await assert.rejects(pending, (error) => error instanceof FrankenConnectCancelledError);
});
