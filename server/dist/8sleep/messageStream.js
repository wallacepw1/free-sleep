
!function(){try{var e="undefined"!=typeof window?window:"undefined"!=typeof global?global:"undefined"!=typeof globalThis?globalThis:"undefined"!=typeof self?self:{},n=(new e.Error).stack;n&&(e._sentryDebugIds=e._sentryDebugIds||{},e._sentryDebugIds[n]="b1671cc1-8f18-5b9d-96e1-fb033c2e607b")}catch(e){}}();
import { once } from 'events';
import binarySplit from 'binary-split';
export class MessageReadTimeoutError extends Error {
    constructor(timeoutMs) {
        super(`Timed out waiting for Franken response after ${timeoutMs}ms`);
        this.name = 'MessageReadTimeoutError';
    }
}
export class MessageStreamInvalidatedError extends Error {
    constructor() {
        super('Franken response stream was invalidated');
        this.name = 'MessageStreamInvalidatedError';
    }
}
/**
 * Frames one outstanding `\n\n`-delimited reply. A late frame stays in this
 * stream until it is read or the stream is invalidated; it is never silently
 * handed to a later read after invalidation.
 */
export class MessageStream {
    splitter;
    queue = [];
    ended = false;
    invalidated = false;
    streamError;
    constructor(readable, separator = Buffer.from('\n\n')) {
        this.splitter = binarySplit(separator);
        this.splitter.on('data', (chunk) => {
            if (this.invalidated)
                return;
            this.queue.push(chunk);
        });
        this.splitter.on('end', () => {
            this.ended = true;
        });
        this.splitter.on('error', (error) => {
            this.streamError = error;
        });
        readable.pipe(this.splitter);
        readable.on('error', (error) => {
            this.splitter.destroy(error);
        });
    }
    /** Drop buffered frames so they cannot be paired with a later command. */
    invalidate() {
        this.invalidated = true;
        this.queue.length = 0;
    }
    takeQueued() {
        if (this.invalidated)
            return undefined;
        return this.queue.shift();
    }
    /**
     * Wait until the splitter emits a frame. A timeout aborts the event listener
     * so a later stream error cannot reject an abandoned promise.
     */
    async waitForData(timeoutMs) {
        if (this.invalidated || this.queue.length > 0)
            return;
        if (timeoutMs === undefined) {
            await once(this.splitter, 'data');
            return;
        }
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);
        try {
            await once(this.splitter, 'data', { signal: controller.signal });
        }
        catch (error) {
            if (this.queue.length > 0)
                return;
            if (controller.signal.aborted) {
                throw new MessageReadTimeoutError(timeoutMs);
            }
            throw error;
        }
        finally {
            clearTimeout(timer);
        }
    }
    /**
     * Read the next frame for the command that is already outstanding.
     * A frame that arrives before the deadline belongs to that command even if
     * it crosses the slow-reply warning. After the deadline the caller must
     * invalidate this stream instead of issuing another read on it.
     */
    async readMessage(options = {}) {
        const startedAt = Date.now();
        const deadlineMs = options.deadlineMs;
        const warnAfterMs = options.warnAfterMs;
        let warned = false;
        while (true) {
            if (this.invalidated)
                throw new MessageStreamInvalidatedError();
            const queued = this.takeQueued();
            if (queued)
                return queued;
            if (this.streamError) {
                const error = this.streamError;
                this.streamError = undefined;
                throw error;
            }
            if (this.ended)
                throw new Error('stream ended');
            const elapsedMs = Date.now() - startedAt;
            if (deadlineMs !== undefined && elapsedMs >= deadlineMs) {
                const lateQueued = this.takeQueued();
                if (lateQueued)
                    return lateQueued;
                throw new MessageReadTimeoutError(deadlineMs);
            }
            const waitMs = this.nextWaitMs(elapsedMs, deadlineMs, warnAfterMs, warned);
            try {
                await this.waitForData(waitMs);
            }
            catch (error) {
                if (this.invalidated)
                    throw new MessageStreamInvalidatedError();
                const arrived = this.takeQueued();
                if (arrived)
                    return arrived;
                const elapsedAfterWait = Date.now() - startedAt;
                if (deadlineMs !== undefined && elapsedAfterWait >= deadlineMs) {
                    throw new MessageReadTimeoutError(deadlineMs);
                }
                if (error instanceof MessageReadTimeoutError
                    && deadlineMs !== undefined
                    && warnAfterMs !== undefined
                    && !warned) {
                    warned = true;
                    options.onSlow?.();
                    continue;
                }
                throw error;
            }
        }
    }
    nextWaitMs(elapsedMs, deadlineMs, warnAfterMs, warned) {
        if (deadlineMs === undefined)
            return undefined;
        const untilDeadline = Math.max(deadlineMs - elapsedMs, 0);
        if (!warned && warnAfterMs !== undefined && elapsedMs < warnAfterMs) {
            return Math.min(untilDeadline, warnAfterMs - elapsedMs);
        }
        return untilDeadline;
    }
}
//# sourceMappingURL=messageStream.js.map
//# debugId=b1671cc1-8f18-5b9d-96e1-fb033c2e607b
