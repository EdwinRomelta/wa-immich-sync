import { describe, expect, it, vi } from 'vitest';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import { AlertStore } from '../src/alert/alertStore.ts';
import { createAlerter } from '../src/alert/alerter.ts';
import { describeGap, detectGaps, lastCapturedByGroup } from '../src/sync/gapDetect.ts';

const HOUR = 3_600_000;

function silentLogger() {
  return { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
}

describe('gap detection to alert, end to end', () => {
  it('alerts once for a silent group and then respects the cooldown', async () => {
    const db = openDb(':memory:');
    const dedup = new DedupStore(db);
    new OutboxStore(db);
    dedup.markDone('a@g.us:1', 'a@g.us', 'asset-1', 'created', 0);

    const sendMessage = vi.fn(async () => ({}));
    const alerter = createAlerter({
      store: new AlertStore(db),
      getSock: () => ({ sendMessage, user: { id: '628123:12@s.whatsapp.net' } }),
      cooldownMs: 6 * HOUR,
      logger: silentLogger(),
      now: () => HOUR * 10,
    });

    const gaps = detectGaps({
      lastKnown: lastCapturedByGroup(db),
      groupJids: ['a@g.us'],
      now: HOUR * 10,
      thresholdMs: HOUR,
    });
    expect(gaps).toHaveLength(1);

    expect(await alerter.raise(`gap:${gaps[0].groupJid}`, describeGap(gaps[0]))).toBe('sent');
    expect(await alerter.raise(`gap:${gaps[0].groupJid}`, describeGap(gaps[0]))).toBe('cooldown');
    expect(sendMessage).toHaveBeenCalledTimes(1);
  });

  it('does not alert when a queued outbox row proves the group is still active', async () => {
    const db = openDb(':memory:');
    new DedupStore(db);
    const outbox = new OutboxStore(db);
    outbox.enqueue({
      messageId: 'a@g.us:2',
      groupJid: 'a@g.us',
      albumName: '',
      filePath: '/tmp/x',
      fileName: 'IMG.jpg',
      mimeType: 'image/jpeg',
      capturedAt: HOUR * 10,
      createdAt: HOUR * 10,
    });

    const gaps = detectGaps({
      lastKnown: lastCapturedByGroup(db),
      groupJids: ['a@g.us'],
      now: HOUR * 10 + 1000,
      thresholdMs: HOUR,
    });
    expect(gaps).toEqual([]);
  });
});
