import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { startStallWatchdog } from '../src/util/stallWatchdog.ts';

describe('startStallWatchdog', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('fires onStall once when no touch within timeout', () => {
    const onStall = vi.fn();
    startStallWatchdog({ timeoutMs: 1000, checkEveryMs: 100, onStall });

    vi.advanceTimersByTime(1100);
    expect(onStall).toHaveBeenCalledTimes(1);
    expect(onStall.mock.calls[0][0]).toBeGreaterThan(1000);

    // Watchdog self-clears — never fires twice.
    vi.advanceTimersByTime(5000);
    expect(onStall).toHaveBeenCalledTimes(1);
  });

  it('touch() defers the stall', () => {
    const onStall = vi.fn();
    const wd = startStallWatchdog({ timeoutMs: 1000, checkEveryMs: 100, onStall });

    vi.advanceTimersByTime(900);
    wd.touch();
    vi.advanceTimersByTime(900);
    expect(onStall).not.toHaveBeenCalled();

    vi.advanceTimersByTime(300);
    expect(onStall).toHaveBeenCalledTimes(1);
  });

  it('stop() prevents firing', () => {
    const onStall = vi.fn();
    const wd = startStallWatchdog({ timeoutMs: 1000, checkEveryMs: 100, onStall });

    wd.stop();
    vi.advanceTimersByTime(10_000);
    expect(onStall).not.toHaveBeenCalled();
  });
});
