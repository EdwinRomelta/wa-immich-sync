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
    const { outbox, dedup } = setup();
    outbox.enqueue(make());
    const row = outbox.due(1_000, 10)[0]!;

    outbox.markSyncedAndRemove(row, 'asset-99', 'created');

    expect(outbox.has('g@g.us:A1')).toBe(false);
    expect(outbox.depth()).toBe(0);
    expect(dedup.has('g@g.us:A1')).toBe(true);
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

  it('lists staged file paths so orphans can be swept', () => {
    const { outbox } = setup();
    outbox.enqueue(make({ messageId: 'g@g.us:A1', filePath: '/tmp/outbox/one' }));
    outbox.enqueue(make({ messageId: 'g@g.us:A2', filePath: '/tmp/outbox/two' }));
    expect(outbox.allFilePaths().sort()).toEqual(['/tmp/outbox/one', '/tmp/outbox/two']);
  });
});
