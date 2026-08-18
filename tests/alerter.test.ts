import { describe, expect, it, vi } from 'vitest';
import { openDb } from '../src/sync/db.ts';
import { AlertStore } from '../src/alert/alertStore.ts';
import { createAlerter, selfJid, type AlertSock } from '../src/alert/alerter.ts';

const COOLDOWN = 21_600_000;

function silentLogger() {
  return { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
}

function setup(opts: { now: () => number; targetJid?: string; sock?: AlertSock | null }) {
  const store = new AlertStore(openDb(':memory:'));
  const sendMessage = vi.fn(async () => ({}));
  const sock: AlertSock =
    opts.sock === undefined ? { sendMessage, user: { id: '628123456:12@s.whatsapp.net' } } : (opts.sock as AlertSock);
  const alerter = createAlerter({
    store,
    getSock: () => (opts.sock === null ? null : sock),
    targetJid: opts.targetJid,
    cooldownMs: COOLDOWN,
    logger: silentLogger(),
    now: opts.now,
  });
  return { alerter, store, sendMessage };
}

describe('selfJid', () => {
  it('strips the device suffix from a linked-device id', () => {
    expect(selfJid('628123456:12@s.whatsapp.net')).toBe('628123456@s.whatsapp.net');
  });

  it('passes through an id with no device suffix', () => {
    expect(selfJid('628123456@s.whatsapp.net')).toBe('628123456@s.whatsapp.net');
  });

  it('returns null for undefined or malformed input', () => {
    expect(selfJid(undefined)).toBeNull();
    expect(selfJid('')).toBeNull();
    expect(selfJid('no-at-sign')).toBeNull();
  });
});

describe('createAlerter', () => {
  it('sends to the bot own number when no target is configured', async () => {
    const { alerter, sendMessage } = setup({ now: () => 1000 });
    expect(await alerter.raise('outbox-depth', 'queue deep')).toBe('sent');
    expect(sendMessage).toHaveBeenCalledWith('628123456@s.whatsapp.net', { text: 'queue deep' });
  });

  it('sends to an explicit target jid when configured', async () => {
    const { alerter, sendMessage } = setup({ now: () => 1000, targetJid: '628999@s.whatsapp.net' });
    await alerter.raise('outbox-depth', 'queue deep');
    expect(sendMessage).toHaveBeenCalledWith('628999@s.whatsapp.net', { text: 'queue deep' });
  });

  it('suppresses a repeat inside the cooldown window', async () => {
    let now = 1000;
    const { alerter, sendMessage } = setup({ now: () => now });
    expect(await alerter.raise('outbox-depth', 'first')).toBe('sent');
    now += COOLDOWN - 1;
    expect(await alerter.raise('outbox-depth', 'second')).toBe('cooldown');
    expect(sendMessage).toHaveBeenCalledTimes(1);
  });

  it('re-sends once the cooldown has elapsed', async () => {
    let now = 1000;
    const { alerter, sendMessage } = setup({ now: () => now });
    await alerter.raise('outbox-depth', 'first');
    now += COOLDOWN;
    expect(await alerter.raise('outbox-depth', 'second')).toBe('sent');
    expect(sendMessage).toHaveBeenCalledTimes(2);
  });

  it('cools down each condition independently', async () => {
    const { alerter, sendMessage } = setup({ now: () => 1000 });
    await alerter.raise('outbox-depth', 'a');
    expect(await alerter.raise('outbox-age', 'b')).toBe('sent');
    expect(sendMessage).toHaveBeenCalledTimes(2);
  });

  it('reports no-socket and does not start a cooldown when WhatsApp is down', async () => {
    const { alerter, store } = setup({ now: () => 1000, sock: null });
    expect(await alerter.raise('outbox-depth', 'queue deep')).toBe('no-socket');
    expect(store.lastSentAt('outbox-depth')).toBeNull();
  });

  it('does not start a cooldown when the send itself throws', async () => {
    const store = new AlertStore(openDb(':memory:'));
    const alerter = createAlerter({
      store,
      getSock: () => ({
        sendMessage: async () => {
          throw new Error('socket closed');
        },
        user: { id: '628123456:12@s.whatsapp.net' },
      }),
      cooldownMs: COOLDOWN,
      logger: silentLogger(),
      now: () => 1000,
    });
    expect(await alerter.raise('outbox-depth', 'queue deep')).toBe('send-failed');
    expect(store.lastSentAt('outbox-depth')).toBeNull();
  });

  it('reports no-socket when the socket has no resolvable user id', async () => {
    const { alerter } = setup({
      now: () => 1000,
      sock: { sendMessage: vi.fn(async () => ({})), user: null },
    });
    expect(await alerter.raise('outbox-depth', 'queue deep')).toBe('no-socket');
  });
});
