import { once } from 'events';
import { unlink } from 'fs/promises';
import { createServer, Server, Socket } from 'net';
import logger from '../logger.js';

interface ConnectionWaiter {
  resolve: (socket: Socket) => void;
  reject: (error: Error) => void;
}

export class UnixSocketServer {
  private readonly pendingConnections: Socket[] = [];
  private waiter: ConnectionWaiter | undefined;
  private closed = false;

  public constructor(private readonly server: Server) {
    this.server.on('connection', (socket) => this.handleConnection(socket));
  }

  private handleConnection(socket: Socket) {
    socket.on('error', (error) => logger.error({ error, message: 'Unix socket connection error' }));

    if (this.waiter) {
      const waiter = this.waiter;
      this.waiter = undefined;
      waiter.resolve(socket);
      return;
    }

    while (this.pendingConnections.length > 0) {
      const stale = this.pendingConnections.shift();
      stale?.destroy();
    }

    this.pendingConnections.push(socket);
  }

  /** Abandon the current accept without closing the listen socket or dropping a queued client. */
  public cancelWait(): void {
    const waiter = this.waiter;
    this.waiter = undefined;
    waiter?.reject(new Error('Franken connection wait cancelled'));
  }

  public async close() {
    if (this.closed) return;
    this.closed = true;
    const waiter = this.waiter;
    this.waiter = undefined;
    waiter?.reject(new Error('Unix socket server closed'));
    this.pendingConnections.splice(0).forEach((socket) => socket.destroy());
    const listening = this.server.listening;
    this.server.close();
    if (listening) await once(this.server, 'close');
  }

  public async waitForConnection(): Promise<Socket> {
    const pending = this.pendingConnections.shift();
    if (pending) return pending;

    logger.debug('Waiting for future connection');
    return new Promise<Socket>((resolve, reject) => {
      if (this.waiter) {
        this.waiter.reject(new Error('Franken connection wait replaced'));
      }
      this.waiter = { resolve, reject };
    });
  }

  public static async start(path: string) {
    logger.debug('Creating socket connection...');
    await UnixSocketServer.tryCleanup(path);
    const unixSocketServer = createServer();
    unixSocketServer.on('error', (error) => logger.error({ error, message: 'Unix socket server error' }));

    await new Promise<void>((resolve) => unixSocketServer.listen(path, resolve));

    const socket = new UnixSocketServer(unixSocketServer);
    logger.debug('Created socket connection!');
    return socket;
  }

  private static async tryCleanup(path: string) {
    try {
      await unlink(path);
    } catch (error) {
      if ((error as NodeJS.ErrnoException)?.code === 'ENOENT') return;
      throw error;
    }
  }
}
