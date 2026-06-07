import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.ts';

const KEYS = [
  'WHITELIST_GROUPS',
  'BACKFILL_GROUP_NAME',
  'ALBUM_MODE',
  'SINGLE_ALBUM_NAME',
  'MEDIA_TYPES',
  'BACKFILL',
  'SYNC_REACTION_EMOJI',
];
let saved: Record<string, string | undefined>;

beforeEach(() => {
  saved = {};
  for (const k of KEYS) {
    saved[k] = process.env[k];
    delete process.env[k];
  }
});

afterEach(() => {
  for (const k of KEYS) {
    if (saved[k] === undefined) delete process.env[k];
    else process.env[k] = saved[k];
  }
});

describe('loadConfig', () => {
  it('parses a comma-separated whitelist and applies defaults', () => {
    process.env.WHITELIST_GROUPS = 'Family, Work , 123@g.us';
    const c = loadConfig();
    expect(c.whitelist).toEqual(['Family', 'Work', '123@g.us']);
    expect(c.mediaTypes).toEqual(['image', 'video']);
    expect(c.backfill).toBe(true);
    expect(c.albumMode).toBe('per-group');
    expect(c.backfillGroupName).toBe('wa-immich-backfill');
    expect(c.reactionEmoji).toBeUndefined();
  });

  it('reads SYNC_REACTION_EMOJI, treating empty as off', () => {
    process.env.WHITELIST_GROUPS = 'X';
    process.env.SYNC_REACTION_EMOJI = '🔄';
    expect(loadConfig().reactionEmoji).toBe('🔄');

    process.env.SYNC_REACTION_EMOJI = '   ';
    expect(loadConfig().reactionEmoji).toBeUndefined();
  });

  it('throws when WHITELIST_GROUPS is empty', () => {
    expect(() => loadConfig()).toThrow();
  });

  it('honors overrides for media types, backfill, album mode and group name', () => {
    process.env.WHITELIST_GROUPS = 'X';
    process.env.MEDIA_TYPES = 'image';
    process.env.BACKFILL = 'false';
    process.env.ALBUM_MODE = 'single';
    process.env.SINGLE_ALBUM_NAME = 'All';
    process.env.BACKFILL_GROUP_NAME = 'bf';
    const c = loadConfig();
    expect(c.mediaTypes).toEqual(['image']);
    expect(c.backfill).toBe(false);
    expect(c.albumMode).toBe('single');
    expect(c.singleAlbumName).toBe('All');
    expect(c.backfillGroupName).toBe('bf');
  });
});
