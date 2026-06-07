import { describe, expect, it } from 'vitest';
import { ChatBackfill, OldestAnchors } from '../src/sync/backfill.ts';

const key = (id: string) => ({ remoteJid: 'g@g.us', fromMe: false, id });

describe('OldestAnchors', () => {
  it('keeps the oldest (smallest timestamp) per jid', () => {
    const a = new OldestAnchors();
    a.note('g@g.us', key('a'), 100);
    a.note('g@g.us', key('b'), 50);
    a.note('g@g.us', key('c'), 200);
    expect(a.get('g@g.us')?.tsSec).toBe(50);
    expect(a.get('g@g.us')?.key.id).toBe('b');
  });

  it('tracks anchors per jid independently', () => {
    const a = new OldestAnchors();
    a.note('x@g.us', key('a'), 100);
    a.note('y@g.us', key('b'), 200);
    expect(a.get('x@g.us')?.tsSec).toBe(100);
    expect(a.get('y@g.us')?.tsSec).toBe(200);
  });

  it('ignores invalid input (no jid, no key id, non-positive ts)', () => {
    const a = new OldestAnchors();
    a.note('', key('a'), 100);
    a.note('g@g.us', null, 100);
    a.note('g@g.us', key('a'), 0);
    a.note('g@g.us', key('a'), -5);
    expect(a.get('g@g.us')).toBeUndefined();
  });
});

describe('ChatBackfill', () => {
  it('waits while there is no anchor', () => {
    const c = new ChatBackfill();
    expect(c.step(undefined)).toEqual({ action: 'wait' });
  });

  it('requests an older page while the anchor keeps advancing', () => {
    const c = new ChatBackfill(3);
    expect(c.step({ key: key('a'), tsSec: 100 }).action).toBe('request');
    expect(c.step({ key: key('b'), tsSec: 80 }).action).toBe('request');
    expect(c.step({ key: key('c'), tsSec: 60 }).action).toBe('request');
  });

  it('declares done after maxStalls steps with no progress', () => {
    const c = new ChatBackfill(3);
    c.step({ key: key('a'), tsSec: 100 }); // seed, advances
    const stalled = { key: key('a'), tsSec: 100 };
    expect(c.step(stalled).action).toBe('request'); // stall 1
    expect(c.step(stalled).action).toBe('request'); // stall 2
    expect(c.step(stalled).action).toBe('done'); // stall 3 -> exhausted
    expect(c.done).toBe(true);
    expect(c.step(stalled)).toEqual({ action: 'done' });
  });

  it('resets stall count when progress resumes', () => {
    const c = new ChatBackfill(3);
    c.step({ key: key('a'), tsSec: 100 });
    const same = { key: key('a'), tsSec: 100 };
    c.step(same); // stall 1
    c.step(same); // stall 2
    expect(c.step({ key: key('b'), tsSec: 50 }).action).toBe('request'); // progress -> reset
    expect(c.step({ key: key('b'), tsSec: 50 }).action).toBe('request'); // stall 1 again
    expect(c.done).toBe(false);
  });
});
