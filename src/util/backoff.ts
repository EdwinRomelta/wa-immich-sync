export interface BackoffOpts {
  /** Delay for attempt 0; doubles each attempt. */
  baseMs: number;
  /** Upper bound for a single delay; uncapped when omitted. */
  maxMs?: number;
  /**
   * Randomise the delay by +/- this fraction (0..1). Defaults to 0 so callers
   * that assert exact delays stay deterministic. Use a non-zero ratio when
   * repeated failures could otherwise turn into a fixed-interval retry storm.
   */
  jitterRatio?: number;
  /** Injectable randomness (tests). Expected range [0, 1). */
  random?: () => number;
}

/** Doubling past this saturates any realistic maxMs; keeps 2 ** n finite. */
const MAX_EXPONENT = 30;

/**
 * Exponential backoff delay for a zero-based attempt number, optionally
 * jittered and capped. Shared by `withRetry` (bounded, deterministic) and the
 * WhatsApp reconnect loop (unbounded, jittered) so a prolonged upstream outage
 * cannot turn into a tight retry storm against the shared WhatsApp account.
 */
export function backoffDelayMs(attempt: number, opts: BackoffOpts): number {
  const { baseMs, maxMs, jitterRatio = 0, random = Math.random } = opts;

  const exponent = Math.min(Math.max(0, Math.floor(attempt)), MAX_EXPONENT);
  const capped = clamp(baseMs * 2 ** exponent, maxMs);

  if (jitterRatio <= 0) return Math.round(capped);

  // random() in [0, 1) -> offset spans [-jitterRatio, +jitterRatio] * capped.
  const offset = capped * jitterRatio * (random() * 2 - 1);
  return Math.round(clamp(Math.max(0, capped + offset), maxMs));
}

function clamp(value: number, maxMs: number | undefined): number {
  return maxMs === undefined ? value : Math.min(value, maxMs);
}
