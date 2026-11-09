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

      const waitMs = deadlineMs === undefined
        ? undefined
        : Math.max(deadlineMs - elapsedMs, 0);
      try {
        await this.waitForData(waitMs);
      } catch (error) {
        if (this.invalidated) throw new MessageStreamInvalidatedError();
        const arrived = this.takeQueued();
        if (arrived) return arrived;
        if (deadlineMs !== undefined && Date.now() - startedAt >= deadlineMs) {
          throw new MessageReadTimeoutError(deadlineMs);
        }
        throw error;
      }
    }
  }
}
