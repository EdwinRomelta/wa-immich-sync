import { EventEmitter } from 'node:events';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import pino from 'pino';
import makeWASocket from '@whiskeysockets/baileys';
import { startWaClient } from '../src/wa/client.ts';

/**
 * startWaClient's only previous test constructed a WaClientOptions object
 * literal and immediately invoked the callback it just wrote — no production
 * code ran, so no real regression could ever fail it.
 *
 * This mocks Baileys wholesale: makeWASocket returns an EventEmitter-backed
 * fake socket under test control, and auth/version resolve instantly, so
 * startWaClient's actual `connection.update` handling runs for real. That
 * handling is what src/index.ts:338 depends on —
 * `if (attempt < alertSettings.reconnectFailures) return;` — which only
 * behaves correctly if `attempt` is 1-based and counts consecutive failures
 * since the last successful 'open', not a lifetime-cumulative or per-socket
 * count. If that counter semantics ever regressed (became cumulative, or
 * reset per socket generation instead of per successful open), no test would
 * catch it and the reconnect-failure alert would either never fire or fire
 * on the first blip.
 */
vi.mock('@whiskeysockets/baileys', () => ({
  default: vi.fn(() => ({ ev: new EventEmitter(), ws: new EventEmitter(), end: vi.fn() })),
  DisconnectReason: { loggedOut: 401 },
  fetchLatestBaileysVersion: vi.fn(async () => ({ version: [2, 3000, 0] })),
  useMultiFileAuthState: vi.fn(async () => ({ state: {}, saveCreds: vi.fn() })),
}));

interface FakeSock {
  ev: EventEmitter;
  ws: EventEmitter;
  end: ReturnType<typeof vi.fn>;
}

const sockFactory = vi.mocked(makeWASocket);

/** The fake socket returned by the i-th (0-based) call to makeWASocket. */
function sockAt(i: number): FakeSock {
  return sockFactory.mock.results[i]!.value as unknown as FakeSock;
}

// WaClientOptions.logger is a full pino Logger (client.ts uses it as such
// elsewhere), so a plain { info/warn/error } stub doesn't satisfy the type —
// use a real, silenced pino instance instead.
const logger = pino({ level: 'silent' });

function close(sock: FakeSock, statusCode: number): void {
  sock.ev.emit('connection.update', {
    connection: 'close',
    lastDisconnect: { error: { output: { statusCode } } },
  });
}

function open(sock: FakeSock): void {
  sock.ev.emit('connection.update', { connection: 'open' });
}

beforeEach(() => {
  sockFactory.mockClear();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('startWaClient reconnect scheduling', () => {
  it('invokes onReconnectScheduled when the connection closes for a non-logout reason', async () => {
    vi.useFakeTimers();
    const onReconnectScheduled = vi.fn();

    await startWaClient({
      authDir: '/tmp/does-not-matter',
      syncFullHistory: false,
      logger,
      onMessage: () => {},
      onReconnectScheduled,
    });

    close(sockAt(0), 500);

    expect(onReconnectScheduled).toHaveBeenCalledTimes(1);
    expect(onReconnectScheduled).toHaveBeenCalledWith({
      attempt: 1,
      delayMs: expect.any(Number),
      statusCode: 500,
    });
  });

  it('does not schedule a reconnect after a logout close', async () => {
    vi.useFakeTimers();
    const onReconnectScheduled = vi.fn();

    await startWaClient({
      authDir: '/tmp/does-not-matter',
      syncFullHistory: false,
      logger,
      onMessage: () => {},
      onReconnectScheduled,
    });

    close(sockAt(0), 401); // DisconnectReason.loggedOut, per the mock above

    expect(onReconnectScheduled).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(sockFactory).toHaveBeenCalledTimes(1); // no reconnect socket created
  });

  it('numbers attempt 1-based, carries consecutive failures across socket generations, and resets after a successful open', async () => {
    vi.useFakeTimers();
    const attempts: number[] = [];

    await startWaClient({
      authDir: '/tmp/does-not-matter',
      syncFullHistory: false,
      logger,
      onMessage: () => {},
      onReconnectScheduled: ({ attempt }) => attempts.push(attempt),
    });

    // Generation 1 closes without ever opening -> first-ever failure -> 1.
    close(sockAt(0), 500);
    expect(attempts).toEqual([1]);

    // Let the scheduled reconnect actually run so generation 2 is created.
    await vi.advanceTimersByTimeAsync(10_000);
    expect(sockFactory).toHaveBeenCalledTimes(2);

    // Generation 2 also closes without opening. The count must be
    // consecutive SINCE THE LAST SUCCESSFUL OPEN — carried into generation 2
    // via `{...opts, reconnectAttempt: nextAttempt}` — not reset just
    // because the socket object itself was replaced.
    close(sockAt(1), 500);
    expect(attempts).toEqual([1, 2]);

    await vi.advanceTimersByTimeAsync(20_000);
    expect(sockFactory).toHaveBeenCalledTimes(3);

    // Generation 3 opens successfully, THEN closes: the in-scope counter was
    // reset to 0 by 'open', so this close reports attempt 1 again — not 3.
    open(sockAt(2));
    close(sockAt(2), 500);
    expect(attempts).toEqual([1, 2, 1]);
  });

  it('still schedules the reconnect when onReconnectScheduled throws', async () => {
    vi.useFakeTimers();

    await startWaClient({
      authDir: '/tmp/does-not-matter',
      syncFullHistory: false,
      logger,
      onMessage: () => {},
      onReconnectScheduled: () => {
        throw new Error('alerting failed');
      },
    });

    close(sockAt(0), 500);

    // A throwing callback must not leave the link permanently dead — the one
    // WhatsApp-side fault this alert channel can still report must not
    // itself break reconnection.
    await vi.advanceTimersByTimeAsync(10_000);
    expect(sockFactory).toHaveBeenCalledTimes(2);
  });
});

describe('startWaClient onFrame', () => {
  it('fires onFrame on every inbound WebSocket frame, independent of onMessage/onReady', async () => {
    const onFrame = vi.fn();

    await startWaClient({
      authDir: '/tmp/does-not-matter',
      syncFullHistory: false,
      logger,
      onMessage: () => {},
      onFrame,
    });

    const sock = sockAt(0);
    sock.ws.emit('message');
    sock.ws.emit('message');
    sock.ws.emit('message');

    // No chat message and no connection.update ever fired — proves this is
    // a link-liveness signal, not a repackaging of onMessage/onReady.
    expect(onFrame).toHaveBeenCalledTimes(3);
  });

  it('does not let a throwing onFrame break the stall watchdog touch that precedes it', async () => {
    await startWaClient({
      authDir: '/tmp/does-not-matter',
      syncFullHistory: false,
      logger,
      onMessage: () => {},
      onFrame: () => {
        throw new Error('stamping failed');
      },
    });

    // Must not throw out of the emitter and must not prevent later frames
    // (e.g. a genuine keepalive) from being processed.
    expect(() => sockAt(0).ws.emit('message')).not.toThrow();
  });
});
