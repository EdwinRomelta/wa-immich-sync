export interface RetryOpts {
  /** Extra attempts after the first one. Default 3 (so up to 4 tries total). */
  retries?: number;
  /** Base backoff in ms; doubles each retry (exponential). Default 1000. */
  baseDelayMs?: number;
  /** Upper bound for a single backoff sleep; uncapped when omitted. */
  maxDelayMs?: number;
  /** Called before each retry sleep, with the error and the upcoming attempt number. */
  onRetry?: (err: unknown, attempt: number) => void;
  /** Injectable sleep (eases testing). */
  sleep?: (ms: number) => Promise<void>;
}

const defaultSleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * Run `fn`, retrying transient failures with exponential backoff. Throws the
 * last error if every attempt fails. Used to stop a single network blip
 * (Baileys media download timeout, Immich HTTP hiccup) from permanently
 * dropping an image.
 */
export async function withRetry<T>(fn: () => Promise<T>, opts: RetryOpts = {}): Promise<T> {
  const retries = opts.retries ?? 3;
  const baseDelayMs = opts.baseDelayMs ?? 1000;
  const sleep = opts.sleep ?? defaultSleep;

  let lastErr: unknown;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (attempt === retries) break;
      opts.onRetry?.(err, attempt + 1);
      const delay = baseDelayMs * 2 ** attempt;
      await sleep(opts.maxDelayMs !== undefined ? Math.min(delay, opts.maxDelayMs) : delay);
    }
  }
  throw lastErr;
}
