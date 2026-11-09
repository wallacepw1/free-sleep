
!function(){try{var e="undefined"!=typeof window?window:"undefined"!=typeof global?global:"undefined"!=typeof globalThis?globalThis:"undefined"!=typeof self?self:{},n=(new e.Error).stack;n&&(e._sentryDebugIds=e._sentryDebugIds||{},e._sentryDebugIds[n]="7ab53c90-88cc-5ef4-8bcd-c5bf5a926aae")}catch(e){}}();
import { once } from 'events';
import { unlink } from 'fs/promises';
import { createServer } from 'net';
import logger from '../logger.js';
export class UnixSocketServer {
    server;
    pendingConnections = [];
    waiter;
    closed = false;
    constructor(server) {
        this.server = server;
        this.server.on('connection', (socket) => this.handleConnection(socket));
    }
    handleConnection(socket) {
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
    cancelWait() {
        const waiter = this.waiter;
        this.waiter = undefined;
        waiter?.reject(new Error('Franken connection wait cancelled'));
    }
    async close() {
        if (this.closed)
            return;
        this.closed = true;
        const waiter = this.waiter;
        this.waiter = undefined;
        waiter?.reject(new Error('Unix socket server closed'));
        this.pendingConnections.splice(0).forEach((socket) => socket.destroy());
        const listening = this.server.listening;
        this.server.close();
        if (listening)
            await once(this.server, 'close');
    }
    async waitForConnection() {
        const pending = this.pendingConnections.shift();
        if (pending)
            return pending;
        logger.debug('Waiting for future connection');
        return new Promise((resolve, reject) => {
            if (this.waiter) {
                this.waiter.reject(new Error('Franken connection wait replaced'));
            }
            this.waiter = { resolve, reject };
        });
    }
    static async start(path) {
        logger.debug('Creating socket connection...');
        await UnixSocketServer.tryCleanup(path);
        const unixSocketServer = createServer();
        unixSocketServer.on('error', (error) => logger.error({ error, message: 'Unix socket server error' }));
        await new Promise((resolve) => unixSocketServer.listen(path, resolve));
        const socket = new UnixSocketServer(unixSocketServer);
        logger.debug('Created socket connection!');
        return socket;
    }
    static async tryCleanup(path) {
        try {
            await unlink(path);
        }
        catch (error) {
            if (error?.code === 'ENOENT')
                return;
            throw error;
        }
    }
}
//# sourceMappingURL=unixSocketServer.js.map
//# debugId=7ab53c90-88cc-5ef4-8bcd-c5bf5a926aae
