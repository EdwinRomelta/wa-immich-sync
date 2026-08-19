import { describe, expect, it } from 'vitest';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import { describeGap, detectGaps, lastCapturedByGroup } from '../src/sync/gapDetect.ts';

const HOUR = 3_600_000;

function setup() {
  const db = openDb(':memory:');
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  return { db, dedup, outbox };
}

describe('lastCapturedByGroup', () => {
  it('is empty when nothing has been seen', () => {
    const { db } = setup();
    expect(lastCapturedByGroup(db).size).toBe(0);
  });

  it('uses captured_at from synced rows', () => {
    const { db, dedup } = setup();
    dedup.markDone('g@g.us:A1', 'g@g.us', 'asset-1', 'created', 5000);
    expect(lastCapturedByGroup(db).get('g@g.us')).toBe(5000);
  });

  it('falls back to created_at for pre-migration rows with a null captured_at', () => {
    const { db } = setup();
    db.prepare(
      `INSERT INTO synced (message_id, group_jid, immich_asset_id, status, created_at, captured_at)
       VALUES ('g@g.us:OLD', 'g@g.us', 'asset-old', 'created', 7000, NULL)`,
    ).run();
    expect(lastCapturedByGroup(db).get('g@g.us')).toBe(7000);
  });

  it('counts still-pending outbox rows, so a stuck queue is not read as silence', () => {
    const { db, outbox } = setup();
    outbox.enqueue({
      messageId: 'g@g.us:A1',
      groupJid: 'g@g.us',
      albumName: '',
      filePath: '/tmp/x',
      fileName: 'IMG.jpg',
      mimeType: 'image/jpeg',
      capturedAt: 9000,
      createdAt: 9000,
    });
    expect(lastCapturedByGroup(db).get('g@g.us')).toBe(9000);
  });

  it('takes the newest across both tables', () => {
    const { db, dedup, outbox } = setup();
    dedup.markDone('g@g.us:A1', 'g@g.us', 'asset-1', 'created', 5000);
    outbox.enqueue({
      messageId: 'g@g.us:A2',
      groupJid: 'g@g.us',
      albumName: '',
      filePath: '/tmp/x',
      fileName: 'IMG.jpg',
      mimeType: 'image/jpeg',
      capturedAt: 9000,
      createdAt: 9000,
    });
    expect(lastCapturedByGroup(db).get('g@g.us')).toBe(9000);
  });

  it('keeps groups separate', () => {
    const { db, dedup } = setup();
    dedup.markDone('a@g.us:1', 'a@g.us', 'asset-1', 'created', 5000);
    dedup.markDone('b@g.us:1', 'b@g.us', 'asset-2', 'created', 8000);
    const map = lastCapturedByGroup(db);
    expect(map.get('a@g.us')).toBe(5000);
    expect(map.get('b@g.us')).toBe(8000);
  });
});

describe('detectGaps', () => {
  const groupJids = ['a@g.us', 'b@g.us'];

  it('reports a group silent for longer than the threshold', () => {
    const lastKnown = new Map([['a@g.us', 0]]);
    const gaps = detectGaps({ lastKnown, groupJids, now: HOUR + 1, thresholdMs: HOUR });
    expect(gaps).toEqual([{ groupJid: 'a@g.us', lastKnownAt: 0, silentForMs: HOUR + 1 }]);
  });

  it('does not report a group inside the threshold', () => {
    const lastKnown = new Map([['a@g.us', 0]]);
    expect(detectGaps({ lastKnown, groupJids, now: HOUR, thresholdMs: HOUR })).toEqual([]);
  });

  it('never reports a group that has never synced anything', () => {
    // A newly whitelisted group has no baseline, so infinite silence is not
    // evidence of a gap — it would otherwise alert forever.
    expect(detectGaps({ lastKnown: new Map(), groupJids, now: HOUR * 100, thresholdMs: HOUR })).toEqual([]);
  });

  it('ignores groups that are no longer whitelisted', () => {
    const lastKnown = new Map([['gone@g.us', 0]]);
    expect(detectGaps({ lastKnown, groupJids, now: HOUR * 100, thresholdMs: HOUR })).toEqual([]);
  });

  it('reports the longest silence first', () => {
    const lastKnown = new Map([
      ['a@g.us', HOUR * 5],
      ['b@g.us', 0],
    ]);
    const gaps = detectGaps({ lastKnown, groupJids, now: HOUR * 10, thresholdMs: HOUR });
    expect(gaps.map((g) => g.groupJid)).toEqual(['b@g.us', 'a@g.us']);
  });
});

describe('describeGap', () => {
  it('renders hours and an ISO last-seen time', () => {
    const text = describeGap({ groupJid: 'a@g.us', lastKnownAt: 0, silentForMs: HOUR * 3 });
    expect(text).toContain('a@g.us');
    expect(text).toContain('3h');
    expect(text).toContain(new Date(0).toISOString());
  });
});
