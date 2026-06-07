import { describe, expect, it } from 'vitest';
import { resolveWhitelist } from '../src/wa/groupResolver.ts';

const groups = [
  { id: '111@g.us', subject: 'Family' },
  { id: '222@g.us', subject: 'Work' },
  { id: '333@g.us', subject: 'Family' }, // duplicate name
];

describe('resolveWhitelist', () => {
  it('matches by exact name', () => {
    const r = resolveWhitelist(groups, ['Work']);
    expect(r.resolved).toEqual([{ jid: '222@g.us', name: 'Work' }]);
    expect(r.warnings).toEqual([]);
  });

  it('matches by jid', () => {
    const r = resolveWhitelist(groups, ['111@g.us']);
    expect(r.resolved).toEqual([{ jid: '111@g.us', name: 'Family' }]);
  });

  it('resolves a duplicate name to ALL matches, with a warning', () => {
    const r = resolveWhitelist(groups, ['Family']);
    expect(r.resolved.map((g) => g.jid).sort()).toEqual(['111@g.us', '333@g.us']);
    expect(r.warnings.some((w) => w.includes('matches 2 groups'))).toBe(true);
  });

  it('warns on an unknown name and an unknown jid, resolving nothing', () => {
    const r = resolveWhitelist(groups, ['Nope', '999@g.us']);
    expect(r.resolved).toEqual([]);
    expect(r.warnings).toHaveLength(2);
  });

  it('de-duplicates when the same group is given by both name and jid', () => {
    const r = resolveWhitelist(groups, ['Work', '222@g.us']);
    expect(r.resolved).toEqual([{ jid: '222@g.us', name: 'Work' }]);
  });

  it('trims entries and ignores empties', () => {
    const r = resolveWhitelist(groups, ['  Work  ', '']);
    expect(r.resolved).toEqual([{ jid: '222@g.us', name: 'Work' }]);
  });
});
