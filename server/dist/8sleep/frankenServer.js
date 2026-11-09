
!function(){try{var e="undefined"!=typeof window?window:"undefined"!=typeof global?global:"undefined"!=typeof globalThis?globalThis:"undefined"!=typeof self?self:{},n=(new e.Error).stack;n&&(e._sentryDebugIds=e._sentryDebugIds||{},e._sentryDebugIds[n]="d371733f-5d43-5f50-826f-ed6944ba7e30")}catch(e){}}();
import { SequentialQueue } from './sequentialQueue.js';
import { MessageReadTimeoutError, MessageStream } from './messageStream.js';
import { frankenCommands } from './deviceApi.js';
import { UnixSocketServer } from './unixSocketServer.js';
import logger from '../logger.js';
import { loadDeviceStatus } from './loadDeviceStatus.js';
import config from '../config.js';
import { toPromise, wait } from './promises.js';
/**
 * Pod 3 DEVICE_STATUS is usually tens of milliseconds. One completed at 9386ms.
 * A 10s close is not proof the reply will never arrive: on the 8:25 PM CST
 * deploy, that close was followed by minutes with no client. Log at 10s and
 * keep reading the same outstanding command until the deadline. The default
 * deadline is 20s. Set FRANKEN_RESPONSE_DEADLINE_MS from 10s through 30s.
 * The verified POST /api/deviceStatus - 204 - 75ms line is an HTTP 204 in 75ms.
 * A longer deadline is a measurement window, not an observed 20s success.
 */
export const FRANKEN_SLOW_REPLY_WARN_MS = 10_000;
export const FRANKEN_RESPONSE_DEADLINE_DEFAULT_MS = 20_000;
export const FRANKEN_RESPONSE_DEADLINE_MAX_MS = 30_000;
/** API calls fail here if no client is connected, instead of holding the HTTP response open. */
export const FRANKEN_REQUEST_CONNECT_BUDGET_MS = 15_000;
const FRANKEN_RESPONSE_DELAY_MS = 10;
const FRANKEN_CONNECTION_TIMEOUT_MS = 25_000;
const FRANKEN_CONNECTION_MAX_ATTEMPTS = 10;
export function resolveResponseDeadlineMs(raw = process.env.FRANKEN_RESPONSE_DEADLINE_MS) {
    if (raw === undefined || raw.trim() === '')
        return FRANKEN_RESPONSE_DEADLINE_DEFAULT_MS;
    const parsed = Number(raw);
    if (!Number.isFinite(parsed))
        return FRANKEN_RESPONSE_DEADLINE_DEFAULT_MS;
    const bounded = Math.min(FRANKEN_RESPONSE_DEADLINE_MAX_MS, Math.max(FRANKEN_SLOW_REPLY_WARN_MS, parsed));
    return Math.floor(bounded);
}
export const DEFAULT_FRANKEN_TIMING = {
    slowReplyWarnMs: FRANKEN_SLOW_REPLY_WARN_MS,
    responseDeadlineMs: resolveResponseDeadlineMs(),
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
export class FrankenRequestConnectBudgetError extends Error {
    constructor(budgetMs) {
        super(`No Franken client for this request after ${budgetMs}ms. The listen socket is still waiting.`);
        this.name = 'FrankenRequestConnectBudgetError';
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
        try {
            const response = await this.messageStream.readMessage({
                deadlineMs: this.timing.responseDeadlineMs,
                warnAfterMs: this.timing.slowReplyWarnMs,
                onSlow: () => {
                    logger.warn(`Franken reply is later than ${this.timing.slowReplyWarnMs}ms. `
                        + `Still waiting for this command only. message: ${message}. `
                        + `deadlineMs: ${this.timing.responseDeadlineMs}`);
                },
            });
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
    linkListener;
    constructor(socketPath, timing = DEFAULT_FRANKEN_TIMING) {
        this.socketPath = socketPath;
        this.timing = timing;
    }
    setLinkListener(listener) {
        this.linkListener = listener;
    }
    connect(budgetMs) {
        const pending = this.beginConnect();
        if (budgetMs === undefined)
            return pending;
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                reject(new FrankenRequestConnectBudgetError(budgetMs));
            }, budgetMs);
            pending.then((franken) => {
                clearTimeout(timer);
                resolve(franken);
            }, (error) => {
                clearTimeout(timer);
                reject(error);
            });
        });
    }
    beginConnect() {
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
     * UnixSocketServer.start already replaces a stale socket file once. Unlinking
     * it again after 25s is what delayed the first client on the 8:25 PM deploy.
     * Later attempts keep the same listen socket.
     */
    async handleConnectionTimeout(attempt, maxAttempts, error) {
        logger.warn('Franken client not connected. Keeping the listen socket so the device can reconnect '
            + 'without a subsystem restart.');
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
export function callFranken(operation, options) {
    return callOnFranken(() => defaultConnection.connect(options?.connectBudgetMs), operation);
}
export function connectFranken(options) {
    return defaultConnection.connect(options?.connectBudgetMs);
}
export async function disconnectFranken() {
    await defaultConnection.disconnect();
}
//# sourceMappingURL=frankenServer.js.map
//# debugId=d371733f-5d43-5f50-826f-ed6944ba7e30
