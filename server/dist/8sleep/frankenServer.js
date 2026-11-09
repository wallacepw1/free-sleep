
!function(){try{var e="undefined"!=typeof window?window:"undefined"!=typeof global?global:"undefined"!=typeof globalThis?globalThis:"undefined"!=typeof self?self:{},n=(new e.Error).stack;n&&(e._sentryDebugIds=e._sentryDebugIds||{},e._sentryDebugIds[n]="13f1defe-d61b-5369-a132-0c0cc2e7ea8e")}catch(e){}}();
import { SequentialQueue } from './sequentialQueue.js';
import { MessageReadTimeoutError, MessageStream, } from './messageStream.js';
import { frankenCommands } from './deviceApi.js';
import { UnixSocketServer } from './unixSocketServer.js';
import logger from '../logger.js';
import { loadDeviceStatus } from './loadDeviceStatus.js';
import config from '../config.js';
import { toPromise, wait } from './promises.js';
/**
 * Pod 3 normally answers DEVICE_STATUS in tens of milliseconds. Replies have
 * no correlation id, so only the outstanding write may consume the next frame.
 * Warn at 10s and keep reading that same write until 30s. A fresh-boot
 * RIGHT_TEMP_DURATION 43200 reply completed at 20475ms, so a 10s close cuts
 * off a reply that still belongs to that command. 30s still fails a missing
 * reply. Do not treat the extra wait as success.
 */
export const FRANKEN_SLOW_REPLY_WARN_MS = 10_000;
export const FRANKEN_RESPONSE_DEADLINE_MS = 30_000;
const FRANKEN_RESPONSE_DELAY_MS = 10;
const FRANKEN_CONNECTION_TIMEOUT_MS = 25_000;
const FRANKEN_CONNECTION_MAX_ATTEMPTS = 10;
export const DEFAULT_FRANKEN_TIMING = {
    slowReplyWarnMs: FRANKEN_SLOW_REPLY_WARN_MS,
    responseDeadlineMs: FRANKEN_RESPONSE_DEADLINE_MS,
    responseDelayMs: FRANKEN_RESPONSE_DELAY_MS,
    connectionTimeoutMs: FRANKEN_CONNECTION_TIMEOUT_MS,
    connectionMaxAttempts: FRANKEN_CONNECTION_MAX_ATTEMPTS,
};
export class FrankenSocketClosedError extends Error {
    constructor() {
        super('Franken socket was invalidated before this command was sent');
        this.name = 'FrankenSocketClosedError';
    }
}
/** True when Node rejected socket.write because the stream was already destroyed. */
export function isDestroyedStreamError(error) {
    if (typeof error !== 'object' || error === null)
        return false;
    const code = error.code;
    if (code === 'ERR_STREAM_DESTROYED')
        return true;
    return error instanceof Error
        && error.message.includes('Cannot call write after a stream was destroyed');
}
class FrankenConnectionTimeoutError extends Error {
    constructor() {
        super('Timed out waiting for Franken hardware connection');
        this.name = 'FrankenConnectionTimeoutError';
    }
}
class FrankenConnectionFailedError extends Error {
    lastError;
    constructor(attempts, lastError) {
        super(`Unable to connect to Franken hardware after ${attempts} attempts`);
        this.lastError = lastError;
        this.name = 'FrankenConnectionFailedError';
    }
}
export class FrankenConnectCancelledError extends Error {
    constructor() {
        super('Franken connect cancelled');
        this.name = 'FrankenConnectCancelledError';
    }
}
export class Franken {
    socket;
    messageStream;
    sequentialQueue;
    timing;
    onInvalidated;
    failed = false;
    /**
     * Runs after the writable check and before socket.write.
     * Tests use it to destroy the socket in that gap.
     */
    beforeWrite;
    constructor(socket, messageStream, sequentialQueue, timing, onInvalidated) {
        this.socket = socket;
        this.messageStream = messageStream;
        this.sequentialQueue = sequentialQueue;
        this.timing = timing;
        this.onInvalidated = onInvalidated;
    }
    static separator = Buffer.from('\n\n');
    /**
     * Send one command and read its reply. Later commands stay queued until this
     * reply arrives or this socket is invalidated. A command that has not been
     * written yet fails with FrankenSocketClosedError instead of writing to a
     * destroyed socket.
     */
    async sendMessage(message) {
        logger.debug(`Sending message to sock | message: ${message}`);
        if (this.isClosed)
            throw new FrankenSocketClosedError();
        const responseBytes = await this.sequentialQueue.exec(async () => {
            if (this.isClosed)
                throw new FrankenSocketClosedError();
            const requestBytes = Buffer.concat([Buffer.from(message), Franken.separator]);
            try {
                await this.write(requestBytes);
            }
            catch (error) {
                this.invalidate('Franken socket write failed before a reply was read');
                if (error instanceof FrankenSocketClosedError || isDestroyedStreamError(error)) {
                    if (isDestroyedStreamError(error)) {
                        logger.warn('Cannot call write after a stream was destroyed. The command was not sent. '
                            + `message: ${message}`);
                    }
                    throw new FrankenSocketClosedError();
                }
                throw error;
            }
            return this.readOutstandingReply(message);
        });
        const response = responseBytes.toString();
        logger.debug(`Message sent successfully to sock | message: ${message}`);
        return response;
    }
    async readOutstandingReply(message) {
        const startedAt = Date.now();
        const readOptions = {
            deadlineMs: this.timing.responseDeadlineMs,
            warnAfterMs: this.timing.slowReplyWarnMs,
            onSlow: () => {
                logger.warn(`Franken reply is later than ${this.timing.slowReplyWarnMs}ms. `
                    + `Still waiting for this command only. message: ${message}`);
            },
        };
        try {
            const response = await this.messageStream.readMessage(readOptions);
            const elapsedMs = Date.now() - startedAt;
            if (elapsedMs >= this.timing.slowReplyWarnMs) {
                logger.warn(`Franken reply matched the outstanding command after ${elapsedMs}ms. message: ${message}`);
            }
            if (this.timing.responseDelayMs > 0) {
                await wait(this.timing.responseDelayMs);
            }
            return response;
        }
        catch (error) {
            if (!(error instanceof FrankenSocketClosedError)) {
                const reason = error instanceof MessageReadTimeoutError
                    ? `Timed out waiting for Franken response after ${this.timing.responseDeadlineMs}ms. message: ${message}`
                    : 'Franken socket failed before the outstanding reply was read';
                this.invalidate(reason);
            }
            throw error;
        }
    }
    /** Destroy the client socket and discard any frame that arrives after this command. */
    invalidate(reason) {
        if (this.failed) {
            this.close();
            return;
        }
        this.failed = true;
        this.messageStream.invalidate();
        logger.warn(`${reason}. Closing the client socket so a late reply cannot be matched to a later command.`);
        this.close();
        this.onInvalidated?.(reason);
    }
    tryStripNewlines(arg) {
        const containsNewline = arg.indexOf('\n') >= 0;
        if (!containsNewline)
            return arg;
        return arg.replace(/\n/gm, '');
    }
    async callFunction(command, arg) {
        logger.debug(`Calling function | command: ${command} | arg: ${arg}`);
        const commandNumber = frankenCommands[command];
        const cleanedArg = this.tryStripNewlines(arg);
        logger.debug(`commandNumber: ${commandNumber}`);
        logger.debug(`cleanedArg: ${cleanedArg}`);
        await this.sendMessage(`${commandNumber}\n${cleanedArg}`);
    }
    async getDeviceStatus(getGestures = false) {
        const command = 'DEVICE_STATUS';
        const commandNumber = frankenCommands[command];
        const response = await this.sendMessage(commandNumber);
        return await loadDeviceStatus(response, getGestures);
    }
    close() {
        const socket = this.socket;
        if (!socket.destroyed)
            socket.destroy();
    }
    get isClosed() {
        return this.failed || this.socket.destroyed || !this.socket.writable;
    }
    static fromSocket(socket, timing = DEFAULT_FRANKEN_TIMING, onInvalidated) {
        const messageStream = new MessageStream(socket, Franken.separator);
        return new Franken(socket, messageStream, new SequentialQueue(), timing, onInvalidated);
    }
    async write(data) {
        if (this.isClosed)
            throw new FrankenSocketClosedError();
        this.beforeWrite?.();
        try {
            await toPromise((callback) => {
                this.socket.write(data, callback);
            });
        }
        catch (error) {
            // Node throws ERR_STREAM_DESTROYED synchronously and does not accept the bytes.
            if (isDestroyedStreamError(error)) {
                logger.warn('Cannot call write after a stream was destroyed. The command was not sent.');
                throw new FrankenSocketClosedError();
            }
            if (this.isClosed)
                throw new FrankenSocketClosedError();
            throw error;
        }
    }
}
class FrankenServer {
    server;
    constructor(server) {
        this.server = server;
    }
    async close() {
        logger.debug('Closing FrankenServer socket...');
        await this.server.close();
    }
    cancelWait() {
        this.server.cancelWait();
    }
    async waitForSocket() {
        const socket = await this.server.waitForConnection();
        logger.debug('FrankenServer connected');
        return socket;
    }
    static async start(path) {
        logger.debug(`Creating franken server on socket: ${path}`);
        const unixSocketServer = await UnixSocketServer.start(path);
        return new FrankenServer(unixSocketServer);
    }
}
/**
 * One listen socket and the currently accepted Franken client.
 * A slow or missing reply closes the client only. The listen socket stays in
 * place so frank can reconnect. It is rebound once, and only if no client has
 * ever connected, to recover a stale socket file at startup.
 */
export class FrankenConnection {
    socketPath;
    timing;
    server;
    franken;
    connectPromise;
    generation = 0;
    hadClient = false;
    rebindCount = 0;
    linkListener;
    constructor(socketPath, timing = DEFAULT_FRANKEN_TIMING) {
        this.socketPath = socketPath;
        this.timing = timing;
    }
    get listenSocketRebinds() {
        return this.rebindCount;
    }
    setLinkListener(listener) {
        this.linkListener = listener;
    }
    connect() {
        if (this.franken && !this.franken.isClosed)
            return Promise.resolve(this.franken);
        this.franken = undefined;
        if (this.connectPromise)
            return this.connectPromise;
        const generation = this.generation;
        const promise = this.connectLoop(generation).finally(() => {
            if (this.connectPromise === promise)
                this.connectPromise = undefined;
        });
        this.connectPromise = promise;
        return promise;
    }
    async disconnect() {
        this.generation += 1;
        this.connectPromise = undefined;
        await this.shutdown();
    }
    notify(state, message) {
        try {
            this.linkListener?.(state, message);
        }
        catch (error) {
            logger.error(error);
        }
    }
    /** Accept the next client, or time out without accepting a socket into a discarded promise. */
    waitForClient(server) {
        if (!this.timing.connectionTimeoutMs)
            return server.waitForSocket();
        return new Promise((resolve, reject) => {
            let settled = false;
            const timer = setTimeout(() => {
                if (settled)
                    return;
                settled = true;
                server.cancelWait();
                logger.warn(`No Franken client connected after ${this.timing.connectionTimeoutMs}ms`);
                reject(new FrankenConnectionTimeoutError());
            }, this.timing.connectionTimeoutMs);
            server.waitForSocket().then((socket) => {
                if (settled) {
                    socket.destroy();
                    return;
                }
                settled = true;
                clearTimeout(timer);
                resolve(socket);
            }).catch((error) => {
                if (settled)
                    return;
                settled = true;
                clearTimeout(timer);
                reject(error);
            });
        });
    }
    async connectLoop(generation) {
        const maxAttempts = this.timing.connectionMaxAttempts;
        for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
            if (generation !== this.generation)
                throw new FrankenConnectCancelledError();
            if (!this.server) {
                this.server = await FrankenServer.start(this.socketPath);
                logger.debug('FrankenServer started');
            }
            if (attempt > 1 || this.hadClient) {
                this.notify('retrying', `Waiting for Franken hardware connection, attempt ${attempt} of ${maxAttempts}`);
            }
            try {
                logger.debug(`Waiting for Franken hardware connection... attempt ${attempt}/${maxAttempts}`);
                const socket = await this.waitForClient(this.server);
                if (generation !== this.generation) {
                    socket.destroy();
                    throw new FrankenConnectCancelledError();
                }
                return this.acceptClient(socket);
            }
            catch (error) {
                if (generation !== this.generation || error instanceof FrankenConnectCancelledError) {
                    throw new FrankenConnectCancelledError();
                }
                if (!(error instanceof FrankenConnectionTimeoutError)) {
                    await this.shutdown();
                    const message = error instanceof Error ? error.message : String(error);
                    this.notify('failed', message);
                    throw error;
                }
                await this.handleConnectionTimeout(attempt, maxAttempts, error);
            }
        }
        throw new FrankenConnectionFailedError(maxAttempts, undefined);
    }
    acceptClient(socket) {
        const franken = Franken.fromSocket(socket, this.timing, (reason) => {
            if (this.franken === franken)
                this.franken = undefined;
            this.notify('failed', reason);
        });
        this.franken = franken;
        this.hadClient = true;
        this.notify('healthy', '');
        logger.info('Franken socket connected');
        return franken;
    }
    /**
     * A missing client used to close and unlink dac.sock on every 25s attempt.
     * That restart is what makes frank reinitialize for minutes. Rebind once
     * only when startup has never accepted a client.
     */
    async handleConnectionTimeout(attempt, maxAttempts, error) {
        const rebindStaleStartupSocket = !this.hadClient && this.rebindCount < 1;
        if (rebindStaleStartupSocket) {
            this.rebindCount += 1;
            logger.warn('No Franken client yet. Rebinding the listen socket once in case the socket file is stale.');
            await this.shutdown();
        }
        else {
            logger.warn('Franken client not connected. Keeping the listen socket so the device can reconnect '
                + 'without a subsystem restart.');
        }
        if (attempt < maxAttempts)
            return;
        const failed = new FrankenConnectionFailedError(maxAttempts, error);
        this.notify('failed', failed.message);
        throw failed;
    }
    async shutdown() {
        this.franken?.close();
        this.franken = undefined;
        if (!this.server)
            return;
        const server = this.server;
        this.server = undefined;
        await server.close();
    }
}
const defaultConnection = new FrankenConnection(config.dacSockPath);
export function setFrankenLinkListener(listener) {
    defaultConnection.setLinkListener(listener);
}
/**
 * Run one socket operation. If the socket was invalidated before the command
 * was written, reconnect once and run it on the new client. A timeout is not
 * retried: the write may already have been applied, and a resend would be a
 * second command with nothing to tell the two replies apart.
 */
export async function callOnFranken(connect, operation) {
    const first = await connect();
    try {
        return await operation(first);
    }
    catch (error) {
        if (!(error instanceof FrankenSocketClosedError))
            throw error;
        logger.warn('Franken command was not sent because the socket was invalidated. Reconnecting once.');
        const second = await connect();
        if (second === first)
            throw error;
        return await operation(second);
    }
}
export function callFranken(operation) {
    return callOnFranken(() => defaultConnection.connect(), operation);
}
export function connectFranken() {
    return defaultConnection.connect();
}
export async function disconnectFranken() {
    await defaultConnection.disconnect();
}
//# sourceMappingURL=frankenServer.js.map
//# debugId=13f1defe-d61b-5369-a132-0c0cc2e7ea8e
