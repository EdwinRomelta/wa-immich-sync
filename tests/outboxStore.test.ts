import { describe, expect, it } from 'vitest';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore, type NewOutboxItem } from '../src/sync/outboxStore.ts';

function make(overrides: Partial<NewOutboxItem> = {}): NewOutboxItem {
  return {
    messageId: 'g@g.us:A1',
    groupJid: 'g@g.us',
    albumName: 'Daycare',
    filePath: '/tmp/outbox/g_g.us_A1',
    fileName: 'IMG-1.jpg',
    mimeType: 'image/jpeg',
    capturedAt: 1_700_000_000_000,
    createdAt: 1_700_000_000_000,
    ...overrides,
  };
}

function setup() {
  const db = openDb(':memory:');
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  return { db, dedup, outbox };
}

describe('OutboxStore', () => {
  it('enqueues a row and reports it as present', () => {
    const { outbox } = setup();
    outbox.enqueue(make());
    expect(outbox.has('g@g.us:A1')).toBe(true);
    expect(outbox.depth()).toBe(1);
  });

  it('returns only rows whose next_try_at has arrived', () => {
    const { outbox } = setup();
    outbox.enqueue(make({ messageId: 'g@g.us:ready' }));
    outbox.enqueue(make({ messageId: 'g@g.us:later' }));
    outbox.defer('g@g.us:later', 'immich down', 5_000);

    const due = outbox.due(1_000, 10);
    expect(due.map((r) => r.messageId)).toEqual(['g@g.us:ready']);
  });

  it('respects the batch limit and returns oldest first', () => {
    const { outbox } = setup();
    outbox.enqueue(make({ messageId: 'g@g.us:B', createdAt: 200 }));
    outbox.enqueue(make({ messageId: 'g@g.us:A', createdAt: 100 }));
    const due = outbox.due(1_000, 1);
    expect(due.map((r) => r.messageId)).toEqual(['g@g.us:A']);
  });

  it('moves a row into synced and removes it from the outbox atomically', () => {
    const { db, outbox, dedup } = setup();
    outbox.enqueue(make());
    const row = outbox.due(1_000, 10)[0]!;

    outbox.markSyncedAndRemove(row, 'asset-99', 'created');

    expect(outbox.has('g@g.us:A1')).toBe(false);
    expect(outbox.depth()).toBe(0);
    expect(dedup.has('g@g.us:A1')).toBe(true);
    // assetId and status are adjacent same-typed parameters; assert the actual
    // columns so a swapped argument order cannot pass.
    expect(
      db
        .prepare('SELECT group_jid, immich_asset_id, status FROM synced WHERE message_id = ?')
        .get('g@g.us:A1'),
    ).toEqual({ group_jid: 'g@g.us', immich_asset_id: 'asset-99', status: 'created' });
  });

  it('leaves the outbox row intact when the synced write fails', () => {
    // No DedupStore here, so `synced` does not exist and the insert throws.
    // The queue entry must survive: a failed drain may never lose staged work.
    const db = openDb(':memory:');
    const outbox = new OutboxStore(db);
    outbox.enqueue(make());
    const row = outbox.due(1_000, 10)[0]!;

    expect(() => outbox.markSyncedAndRemove(row, 'asset-99', 'created')).toThrow();
    expect(outbox.has('g@g.us:A1')).toBe(true);
    expect(outbox.depth()).toBe(1);
  });

  it('round-trips every column and ignores a duplicate enqueue', () => {
    const { outbox } = setup();
    outbox.enqueue(make());
    outbox.defer('g@g.us:A1', 'boom', 42);
    // A repeat enqueue must not reset attempts — that would retry forever.
    outbox.enqueue(make({ filePath: '/tmp/outbox/moved' }));

    expect(outbox.depth()).toBe(1);
    expect(outbox.due(1_000, 10)[0]).toEqual({
      ...make(),
      attempts: 1,
      lastError: 'boom',
      nextTryAt: 42,
    });
  });

  it('rejects a row with a missing required field instead of dropping it', () => {
    const { outbox } = setup();
    expect(() => outbox.enqueue(make({ mimeType: null as unknown as string }))).toThrow();
    expect(outbox.depth()).toBe(0);
  });

  it('reports an unknown message as absent', () => {
    const { outbox } = setup();
    expect(outbox.has('g@g.us:nope')).toBe(false);
  });

  it('treats a non-positive batch limit as empty rather than unbounded', () => {
    const { outbox } = setup();
    outbox.enqueue(make());
    expect(outbox.due(1_000, -1)).toEqual([]);
  });

  it('increments attempts and records the error when deferring', () => {
    const { outbox } = setup();
    outbox.enqueue(make());
    outbox.defer('g@g.us:A1', 'ECONNREFUSED', 9_999);

    const row = outbox.due(10_000, 10)[0]!;
    expect(row.attempts).toBe(1);
    expect(row.lastError).toBe('ECONNREFUSED');
    expect(row.nextTryAt).toBe(9_999);
  });

  it('drops a row without recording it as synced', () => {
    const { db, outbox, dedup } = setup();
    outbox.enqueue(make());

    outbox.remove('g@g.us:A1');

    expect(outbox.has('g@g.us:A1')).toBe(false);
    expect(outbox.depth()).toBe(0);
    expect(dedup.has('g@g.us:A1')).toBe(false);
    expect(db.prepare('SELECT 1 FROM synced WHERE message_id = ?').get('g@g.us:A1')).toBeUndefined();
  });

  it('tolerates removing a message that is not queued', () => {
    const { outbox } = setup();
    expect(() => outbox.remove('g@g.us:nope')).not.toThrow();
  });

  it('lists staged file paths so orphans can be swept', () => {
    const { outbox } = setup();
    outbox.enqueue(make({ messageId: 'g@g.us:A1', filePath: '/tmp/outbox/one' }));
    outbox.enqueue(make({ messageId: 'g@g.us:A2', filePath: '/tmp/outbox/two' }));
    expect(outbox.allFilePaths().sort()).toEqual(['/tmp/outbox/one', '/tmp/outbox/two']);
  });

  describe('snapshot', () => {
    it('reports an empty outbox with no pending work', () => {
      const { outbox } = setup();
      expect(outbox.snapshot(1_000)).toEqual({
        depth: 0,
        oldestPendingAgeMs: null,
        maxAttempts: 0,
        lastError: null,
      });
    });

    it('reports depth and the oldest pending row age from the earliest created_at', () => {
      const { outbox } = setup();
      outbox.enqueue(make({ messageId: 'g@g.us:newer', createdAt: 5_000 }));
      outbox.enqueue(make({ messageId: 'g@g.us:older', createdAt: 1_000 }));

      const snap = outbox.snapshot(9_000);
      expect(snap.depth).toBe(2);
      expect(snap.oldestPendingAgeMs).toBe(8_000);
    });

    it('reports the highest attempts count across all pending rows', () => {
      const { outbox } = setup();
      outbox.enqueue(make({ messageId: 'g@g.us:A' }));
      outbox.enqueue(make({ messageId: 'g@g.us:B' }));
      outbox.defer('g@g.us:A', 'ECONNREFUSED', 1_000);
      outbox.defer('g@g.us:B', 'ECONNREFUSED', 1_000);
      outbox.defer('g@g.us:B', 'ECONNREFUSED', 2_000);

      expect(outbox.snapshot(3_000).maxAttempts).toBe(2);
    });

    it('reports none as the most recent error before any row has ever failed', () => {
      const { outbox } = setup();
      outbox.enqueue(make());
      expect(outbox.snapshot(1_000).lastError).toBeNull();
    });

    it('reports the last_error of the most recently deferred row', () => {
      const { outbox } = setup();
      outbox.enqueue(make({ messageId: 'g@g.us:A' }));
      outbox.enqueue(make({ messageId: 'g@g.us:B' }));
      // B is deferred to a later next_try_at than A, so it is the more
      // recent failure even though A was enqueued first.
      outbox.defer('g@g.us:A', 'stale error', 1_000);
      outbox.defer('g@g.us:B', 'fresh error', 5_000);

      expect(outbox.snapshot(9_000).lastError).toBe('fresh error');
    });

    it('defaults `now` to the current time when not provided', () => {
      const { outbox } = setup();
      outbox.enqueue(make({ createdAt: Date.now() - 10 }));
      const snap = outbox.snapshot();
      expect(snap.oldestPendingAgeMs).toBeGreaterThanOrEqual(0);
    });
  });
});
