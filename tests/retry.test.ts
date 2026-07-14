import { describe, expect, it, vi } from 'vitest';
import { withRetry } from '../src/util/retry.ts';

const noSleep = async () => {};

describe('withRetry', () => {
  it('returns the result without retrying when the first attempt succeeds', async () => {
    const fn = vi.fn(async () => 'ok');
    const onRetry = vi.fn();
    expect(await withRetry(fn, { sleep: noSleep, onRetry })).toBe('ok');
    expect(fn).toHaveBeenCalledTimes(1);
    expect(onRetry).not.toHaveBeenCalled();
  });

  it('retries transient failures and returns once one attempt succeeds', async () => {
    let calls = 0;
    const fn = vi.fn(async () => {
      calls += 1;
      if (calls < 3) throw new Error('Timed Out');
      return 'recovered';
    });
    const onRetry = vi.fn();
    expect(await withRetry(fn, { retries: 3, sleep: noSleep, onRetry })).toBe('recovered');
    expect(fn).toHaveBeenCalledTimes(3);
    expect(onRetry).toHaveBeenCalledTimes(2);
  });

  it('throws the last error after exhausting all attempts', async () => {
    const fn = vi.fn(async () => {
      throw new Error('still down');
    });
    await expect(withRetry(fn, { retries: 2, sleep: noSleep })).rejects.toThrow('still down');
    expect(fn).toHaveBeenCalledTimes(3); // first attempt + 2 retries
  });

  it('backs off exponentially between attempts', async () => {
    const delays: number[] = [];
    const sleep = async (ms: number) => {
      delays.push(ms);
    };
    const fn = vi.fn(async () => {
      throw new Error('nope');
    });
    await expect(withRetry(fn, { retries: 3, baseDelayMs: 100, sleep })).rejects.toThrow();
    expect(delays).toEqual([100, 200, 400]);
  });

  it('caps the backoff at maxDelayMs so long waits stay bounded', async () => {
    const delays: number[] = [];
    const sleep = async (ms: number) => {
      delays.push(ms);
    };
    const fn = vi.fn(async () => {
      throw new Error('nope');
    });
    await expect(
      withRetry(fn, { retries: 5, baseDelayMs: 100, maxDelayMs: 300, sleep }),
    ).rejects.toThrow();
    expect(delays).toEqual([100, 200, 300, 300, 300]);
  });
});
