import { describe, expect, it } from 'vitest';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { openDb } from '../src/sync/db.ts';

describe('DedupStore', () => {
  it('has() is false before and true after markDone()', () => {
    const s = new DedupStore(':memory:');
    expect(s.has('g@g.us:1')).toBe(false);
    s.markDone('g@g.us:1', 'g@g.us', 'asset-1', 'created');
    expect(s.has('g@g.us:1')).toBe(true);
    s.close();
  });

  it('markDone() is idempotent (INSERT OR REPLACE)', () => {
    const s = new DedupStore(':memory:');
    s.markDone('g@g.us:1', 'g@g.us', 'a1', 'created');
    s.markDone('g@g.us:1', 'g@g.us', 'a1', 'duplicate');
    expect(s.count()).toBe(1);
    s.close();
  });

  it('counts by group', () => {
    const s = new DedupStore(':memory:');
    s.markDone('A:1', 'A', 'x', 'created');
    s.markDone('A:2', 'A', 'y', 'created');
    s.markDone('B:1', 'B', 'z', 'created');
    const byGroup = Object.fromEntries(s.countByGroup().map((r) => [r.group_jid, r.c]));
    expect(byGroup).toEqual({ A: 2, B: 1 });
    s.close();
  });

  it('lastSyncedAt() is null when empty', () => {
    const s = new DedupStore(':memory:');
    expect(s.lastSyncedAt()).toBeNull();
    s.markDone('A:1', 'A', 'x', 'created');
    expect(typeof s.lastSyncedAt()).toBe('number');
    s.close();
  });

  it('accepts a shared Database instance so other stores can share the connection', () => {
    const db = openDb(':memory:');
    const store = new DedupStore(db);
    store.markDone('g@g.us:A1', 'g@g.us', 'asset-1', 'created');
    expect(store.count()).toBe(1);
    // The same connection sees the table.
    const row = db.prepare('SELECT COUNT(*) AS c FROM synced').get() as { c: number };
    expect(row.c).toBe(1);
  });
});
