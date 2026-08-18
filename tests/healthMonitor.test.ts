import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { readHeartbeat } from '../src/health/heartbeat.ts';
import { startHealthMonitor } from '../src/health/monitor.ts';
import type { OutboxSnapshot } from '../src/sync/outboxStore.ts';

const dirs: string[] = [];
function tmp(): string {
  const d = mkdtempSync(join(tmpdir(), 'monitor-test-'));
  dirs.push(d);
  return d;
}
afterEach(() => {
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true });
});

const EMPTY: OutboxSnapshot = { depth: 0, oldestPendingAgeMs: null, maxAttempts: 0, lastError: null };

function setup(snapshot: OutboxSnapshot, opts: { now?: number; waActivity?: number | null } = {}) {
  const heartbeatPath = join(tmp(), 'health.json');
  const raise = vi.fn(async () => 'sent' as const);
  const monitor = startHealthMonitor({
    outbox: { snapshot: vi.fn(() => snapshot) },
    alerter: { raise },
    heartbeatPath,
    waActivity: () => (opts.waActivity === undefined ? 500 : opts.waActivity),
    thresholds: { outboxDepth: 50, outboxAgeMs: 7_200_000 },
    intervalMs: 60_000,
    logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
    now: () => opts.now ?? 1000,
    autoStart: false,
  });
  return { monitor, raise, heartbeatPath };
}

describe('startHealthMonitor', () => {
  it('writes both heartbeat stamps on every tick', async () => {
    const { monitor, heartbeatPath } = setup(EMPTY, { now: 1000, waActivity: 500 });
    await monitor.tick();
    expect(await readHeartbeat(heartbeatPath)).toEqual({ daemon: 1000, wa: 500 });
  });

  it('writes a null wa stamp before WhatsApp has ever connected', async () => {
    const { monitor, heartbeatPath } = setup(EMPTY, { now: 1000, waActivity: null });
    await monitor.tick();
    expect(await readHeartbeat(heartbeatPath)).toEqual({ daemon: 1000, wa: null });
  });

  it('raises no alert on an empty outbox', async () => {
    const { monitor, raise } = setup(EMPTY);
    await monitor.tick();
    expect(raise).not.toHaveBeenCalled();
  });

  it('raises outbox-depth once the depth threshold is reached', async () => {
    const { monitor, raise } = setup({ depth: 50, oldestPendingAgeMs: 1000, maxAttempts: 2, lastError: 'ECONNREFUSED' });
    await monitor.tick();
    expect(raise).toHaveBeenCalledTimes(1);
    const calls = raise.mock.calls as unknown as [string, string][];
    expect(calls[0][0]).toBe('outbox-depth');
    expect(calls[0][1]).toContain('50');
    expect(calls[0][1]).toContain('ECONNREFUSED');
  });

  it('raises outbox-age once the age threshold is reached', async () => {
    const { monitor, raise } = setup({ depth: 1, oldestPendingAgeMs: 7_200_000, maxAttempts: 9, lastError: '503' });
    await monitor.tick();
    expect(raise).toHaveBeenCalledTimes(1);
    const calls = raise.mock.calls as unknown as [string, string][];
    expect(calls[0][0]).toBe('outbox-age');
  });

  it('raises both conditions when both are breached', async () => {
    const { monitor, raise } = setup({ depth: 500, oldestPendingAgeMs: 99_000_000, maxAttempts: 30, lastError: '503' });
    await monitor.tick();
    const calls = raise.mock.calls as unknown as [string, string][];
    expect(calls.map((c) => c[0]).sort()).toEqual(['outbox-age', 'outbox-depth']);
  });

  it('still writes the heartbeat when reading the outbox throws', async () => {
    const heartbeatPath = join(tmp(), 'health.json');
    const monitor = startHealthMonitor({
      outbox: {
        snapshot: () => {
          throw new Error('database is locked');
        },
      },
      alerter: { raise: vi.fn(async () => 'sent' as const) },
      heartbeatPath,
      waActivity: () => 500,
      thresholds: { outboxDepth: 50, outboxAgeMs: 7_200_000 },
      intervalMs: 60_000,
      logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
      now: () => 1000,
      autoStart: false,
    });
    await expect(monitor.tick()).resolves.toBeUndefined();
    expect(await readHeartbeat(heartbeatPath)).toEqual({ daemon: 1000, wa: 500 });
  });

  it('does not reject when the alerter throws', async () => {
    const heartbeatPath = join(tmp(), 'health.json');
    const monitor = startHealthMonitor({
      outbox: { snapshot: () => ({ depth: 99, oldestPendingAgeMs: 1, maxAttempts: 1, lastError: null }) },
      alerter: {
        raise: async () => {
          throw new Error('boom');
        },
      },
      heartbeatPath,
      waActivity: () => 500,
      thresholds: { outboxDepth: 50, outboxAgeMs: 7_200_000 },
      intervalMs: 60_000,
      logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
      now: () => 1000,
      autoStart: false,
    });
    await expect(monitor.tick()).resolves.toBeUndefined();
  });

  it('stop() is safe to call when autoStart is false', () => {
    const { monitor } = setup(EMPTY);
    expect(() => monitor.stop()).not.toThrow();
  });

  it('keeps re-arming the timer loop across ticks when the alerter never settles', async () => {
    // Regression for a real bug: tick() used to `await deps.alerter.raise()`.
    // raise() calls sock.sendMessage, which can hang forever on a half-open
    // WhatsApp socket — a state the stall watchdog exists precisely because
    // it occurs. A hung await meant tick() never resolved, so loop()'s
    // .finally() (which re-arms the setTimeout) never ran again: the
    // heartbeat stopped for good after the first tick that triggered an
    // alert, not just for that one cycle.
    //
    // This has to run through autoStart's real setTimeout loop, not a
    // hand-called tick() — a hand-called tick() never exercises the re-arm
    // path the bug lived in. It also can't use fake timers: writeHeartbeat
    // does real fs I/O, and advancing a fake clock races real (non-timer)
    // async completion, which is exactly what makes such tests flaky (see
    // the vi.mock('node:fs', ...) comment in drain.test.ts for the same
    // tradeoff made there). So instead of asserting an exact tick count
    // against wall-clock time, poll for a real, unbounded fact: the
    // heartbeat keeps moving forward release after release, past a small
    // number of intervals, well beyond the single tick a wedged loop would
    // produce.
    const heartbeatPath = join(tmp(), 'health.json');
    const intervalMs = 5;
    const raise = vi.fn(() => new Promise<never>(() => {})); // never settles
    const monitor = startHealthMonitor({
      outbox: { snapshot: vi.fn(() => ({ depth: 999, oldestPendingAgeMs: 1, maxAttempts: 1, lastError: null })) },
      alerter: { raise },
      heartbeatPath,
      waActivity: () => 500,
      thresholds: { outboxDepth: 50, outboxAgeMs: 7_200_000 },
      intervalMs,
      logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
      autoStart: true,
    });

    const MIN_TICKS = 10;
    const deadline = Date.now() + 3_000;
    while (raise.mock.calls.length < MIN_TICKS) {
      if (Date.now() > deadline) {
        monitor.stop();
        throw new Error(
          `alerter was called only ${raise.mock.calls.length} times in 3s — the timer loop stopped re-arming`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    const beatAtThreshold = await readHeartbeat(heartbeatPath);
    monitor.stop();

    // A wedged loop (the bug) writes exactly one heartbeat, ever, and never
    // calls the alerter more than once. Reaching MIN_TICKS alerter calls at
    // all is already proof the loop kept re-arming; this also confirms the
    // heartbeat file itself was still being rewritten alongside those calls.
    expect(raise.mock.calls.length).toBeGreaterThanOrEqual(MIN_TICKS);
    expect(beatAtThreshold).not.toBeNull();
    expect(beatAtThreshold?.wa).toBe(500);
  });
});
