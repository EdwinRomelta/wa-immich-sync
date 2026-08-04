import { describe, expect, it } from 'vitest';
import { backoffDelayMs } from '../src/util/backoff.ts';

describe('backoffDelayMs', () => {
  it('returns the base delay for the first attempt', () => {
    expect(backoffDelayMs(0, { baseMs: 100 })).toBe(100);
  });

  it('doubles the delay on each subsequent attempt', () => {
    const delays = [0, 1, 2, 3].map((a) => backoffDelayMs(a, { baseMs: 100 }));
    expect(delays).toEqual([100, 200, 400, 800]);
  });

  it('caps the delay at maxMs so reconnects never stall forever', () => {
    const delays = [0, 1, 2, 3, 4].map((a) => backoffDelayMs(a, { baseMs: 100, maxMs: 300 }));
    expect(delays).toEqual([100, 200, 300, 300, 300]);
  });

  it('stays at maxMs for very large attempt counts instead of overflowing', () => {
    expect(backoffDelayMs(1000, { baseMs: 3000, maxMs: 300_000 })).toBe(300_000);
  });

  it('treats negative attempts as the first attempt', () => {
    expect(backoffDelayMs(-5, { baseMs: 100 })).toBe(100);
  });

  it('applies jitter within the configured ratio', () => {
    // random() === 1 -> full positive jitter; 0 -> full negative jitter.
    const high = backoffDelayMs(1, { baseMs: 100, jitterRatio: 0.5, random: () => 1 });
    const low = backoffDelayMs(1, { baseMs: 100, jitterRatio: 0.5, random: () => 0 });
    const mid = backoffDelayMs(1, { baseMs: 100, jitterRatio: 0.5, random: () => 0.5 });
    expect(high).toBe(300); // 200 + 200*0.5
    expect(low).toBe(100); // 200 - 200*0.5
    expect(mid).toBe(200); // no offset
  });

  it('never returns a negative delay even with full negative jitter', () => {
    expect(backoffDelayMs(0, { baseMs: 100, jitterRatio: 1, random: () => 0 })).toBe(0);
  });

  it('never exceeds maxMs once jitter is applied', () => {
    for (let attempt = 0; attempt < 20; attempt += 1) {
      const d = backoffDelayMs(attempt, {
        baseMs: 3000,
        maxMs: 60_000,
        jitterRatio: 0.5,
        random: () => 1,
      });
      expect(d).toBeLessThanOrEqual(60_000);
      expect(d).toBeGreaterThanOrEqual(0);
    }
  });

  it('defaults to no jitter so callers get deterministic delays', () => {
    const a = backoffDelayMs(3, { baseMs: 250 });
    const b = backoffDelayMs(3, { baseMs: 250 });
    expect(a).toBe(b);
    expect(a).toBe(2000);
  });
});
