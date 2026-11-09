import { once } from 'events';
import binarySplit from 'binary-split';
import { Transform } from 'stream';

export class MessageReadTimeoutError extends Error {
  public constructor(timeoutMs: number) {
    super(`Timed out waiting for Franken response after ${timeoutMs}ms`);
    this.name = 'MessageReadTimeoutError';
  }
}

export class MessageStreamInvalidatedError extends Error {
  public constructor() {
    super('Franken response stream was invalidated');
    this.name = 'MessageStreamInvalidatedError';
  }
}

export interface ReadMessageOptions {
  deadlineMs?: number;
  warnAfterMs?: number;
  onSlow?: () => void;
}

/**
 * Frames one outstanding `\n\n`-delimited reply. A late frame stays in this
 * stream until it is read or the stream is invalidated; it is never silently
 * handed to a later read after invalidation.
 */
export class MessageStream {
  private readonly splitter: Transform;
  private readonly queue: Buffer[] = [];
  private ended = false;
  private invalidated = false;
  private streamError: unknown;

  public constructor(
    readable: NodeJS.ReadableStream,
    separator = Buffer.from('\n\n'),
  ) {
    this.splitter = binarySplit(separator);
    this.splitter.on('data', (chunk: Buffer) => {
      if (this.invalidated) return;
      this.queue.push(chunk);
    });
    this.splitter.on('end', () => {
      this.ended = true;
    });
    this.splitter.on('error', (error: unknown) => {
      this.streamError = error;
    });

    readable.pipe(this.splitter);
    readable.on('error', (error: Error) => {
      this.splitter.destroy(error);
    });
  }

  /** Drop buffered frames so they cannot be paired with a later command. */
  public invalidate(): void {
    this.invalidated = true;
    this.queue.length = 0;
  }

  /**
   * Drop complete frames already buffered and any frame that arrives before
   * `quietMs` of silence, up to `maxMs`. Used after a missed reply so a late
   * frame is not read as the next command. The socket stays open.
   */
  public async discardLateFrames(quietMs: number, maxMs: number): Promise<number> {
    const startedAt = Date.now();
    let discarded = 0;
    while (this.queue.length > 0 && !this.invalidated) {
      this.queue.shift();
      discarded += 1;
    }
    while (!this.invalidated && Date.now() - startedAt < maxMs) {
      const elapsedMs = Date.now() - startedAt;
      const sliceMs = Math.min(quietMs, maxMs - elapsedMs);
      if (sliceMs <= 0) break;
      try {
        await this.waitForData(sliceMs);
      } catch (error) {
        if (error instanceof MessageReadTimeoutError) break;
        throw error;
      }
      while (this.queue.length > 0 && !this.invalidated) {
        this.queue.shift();
        discarded += 1;
      }
    }
    return discarded;
  }

  private takeQueued(): Buffer | undefined {
    if (this.invalidated) return undefined;
    return this.queue.shift();
  }

  /**
   * Wait until the splitter emits a frame. A timeout aborts the event listener
   * so a later stream error cannot reject an abandoned promise.
   */
  private async waitForData(timeoutMs?: number): Promise<void> {
    if (this.invalidated || this.queue.length > 0) return;

    if (timeoutMs === undefined) {
      await once(this.splitter, 'data');
      return;
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      await once(this.splitter, 'data', { signal: controller.signal });
    } catch (error) {
      if (this.queue.length > 0) return;
      if (controller.signal.aborted) {
        throw new MessageReadTimeoutError(timeoutMs);
      }
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  /**
   * Read the next frame for the command that is already outstanding.
   * A frame that is already complete when the deadline fires still belongs to
   * that command. After the deadline the caller must invalidate this stream
   * instead of issuing another read on it.
   */
  public async readMessage(options: ReadMessageOptions = {}): Promise<Buffer> {
    const startedAt = Date.now();
    const deadlineMs = options.deadlineMs;
    let warned = false;

    while (true) {
      if (this.invalidated) throw new MessageStreamInvalidatedError();

      const queued = this.takeQueued();
      if (queued) return queued;

      if (this.streamError) {
        const error = this.streamError;
        this.streamError = undefined;
        throw error;
      }

      if (this.ended) throw new Error('stream ended');

      const elapsedMs = Date.now() - startedAt;
      if (deadlineMs !== undefined && elapsedMs >= deadlineMs) {
        const lateQueued = this.takeQueued();
        if (lateQueued) return lateQueued;
        throw new MessageReadTimeoutError(deadlineMs);
      }

      const waitMs = this.nextWaitMs(elapsedMs, deadlineMs, options.warnAfterMs, warned);
      try {
        await this.waitForData(waitMs);
      } catch (error) {
        if (this.invalidated) throw new MessageStreamInvalidatedError();
        const arrived = this.takeQueued();
        if (arrived) return arrived;
        const elapsedAfterWait = Date.now() - startedAt;
        if (deadlineMs !== undefined && elapsedAfterWait >= deadlineMs) {
          throw new MessageReadTimeoutError(deadlineMs);
        }
        // The warn timer can fire before Date.now() reaches the mark. Any
        // timeout before the deadline is only the slow-reply log.
        if (
          error instanceof MessageReadTimeoutError
          && deadlineMs !== undefined
          && elapsedAfterWait < deadlineMs
          && options.warnAfterMs !== undefined
          && !warned
        ) {
          warned = true;
          options.onSlow?.();
          continue;
        }
        throw error;
      }
    }
  }

  /** Wait until the slow-reply mark first, then until the deadline. The mark does not close the socket. */
  private nextWaitMs(
    elapsedMs: number,
    deadlineMs: number | undefined,
    warnAfterMs: number | undefined,
    warned: boolean,
  ): number | undefined {
    if (deadlineMs === undefined) return undefined;
    const untilDeadline = Math.max(deadlineMs - elapsedMs, 0);
    if (!warned && warnAfterMs !== undefined && elapsedMs < warnAfterMs) {
      return Math.min(untilDeadline, Math.max(warnAfterMs - elapsedMs, 0));
    }
    return untilDeadline;
  }
}
