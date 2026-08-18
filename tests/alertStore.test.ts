import { describe, expect, it } from 'vitest';
import { openDb } from '../src/sync/db.ts';
import { AlertStore } from '../src/alert/alertStore.ts';

describe('AlertStore', () => {
  it('returns null for a condition never sent', () => {
    const store = new AlertStore(openDb(':memory:'));
    expect(store.lastSentAt('outbox-depth')).toBeNull();
  });

  it('records and reads back a send time', () => {
    const store = new AlertStore(openDb(':memory:'));
    store.recordSent('outbox-depth', 1000);
    expect(store.lastSentAt('outbox-depth')).toBe(1000);
  });

  it('keeps conditions independent', () => {
    const store = new AlertStore(openDb(':memory:'));
    store.recordSent('outbox-depth', 1000);
    expect(store.lastSentAt('outbox-age')).toBeNull();
  });

  it('overwrites an earlier send time for the same condition', () => {
    const store = new AlertStore(openDb(':memory:'));
    store.recordSent('outbox-depth', 1000);
    store.recordSent('outbox-depth', 2000);
    expect(store.lastSentAt('outbox-depth')).toBe(2000);
  });

  it('survives reconstruction on the same connection', () => {
    const db = openDb(':memory:');
    new AlertStore(db).recordSent('outbox-depth', 1000);
    expect(new AlertStore(db).lastSentAt('outbox-depth')).toBe(1000);
  });

  it('lists every recorded condition', () => {
    const store = new AlertStore(openDb(':memory:'));
    store.recordSent('outbox-age', 2000);
    store.recordSent('outbox-depth', 1000);
    expect(store.all()).toEqual([
      { condition: 'outbox-age', lastSentAt: 2000 },
      { condition: 'outbox-depth', lastSentAt: 1000 },
    ]);
  });
});
