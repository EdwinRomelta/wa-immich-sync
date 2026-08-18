import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { getDedupDb, getDrainSettings, getHealthFile, getOutboxDir, getWaAuthDir, loadConfig, outboxGuards } from '../src/config.ts';

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

describe('outbox settings', () => {
  it('defaults the outbox directory', () => {
    delete process.env.OUTBOX_DIR;
    expect(getOutboxDir()).toBe('./data/outbox');
  });

  it('never lets the backoff ceiling fall below the floor', () => {
    // backoffDelayMs clamps to maxMs, so a ceiling under the floor would
    // silently flatten exponential backoff into a fixed retry delay.
    process.env.DRAIN_BASE_BACKOFF_MS = '30000';
    process.env.DRAIN_MAX_BACKOFF_MS = '1000';
    const s = getDrainSettings();
    expect(s.maxBackoffMs).toBe(30_000);
    delete process.env.DRAIN_BASE_BACKOFF_MS;
    delete process.env.DRAIN_MAX_BACKOFF_MS;
  });

  it('honours OUTBOX_DIR when set', () => {
    process.env.OUTBOX_DIR = '/mnt/staging';
    expect(getOutboxDir()).toBe('/mnt/staging');
    delete process.env.OUTBOX_DIR;
  });

  it('uses the documented drain defaults', () => {
    for (const k of [
      'DRAIN_INTERVAL_MS',
      'DRAIN_BATCH_SIZE',
      'DRAIN_BASE_BACKOFF_MS',
      'DRAIN_MAX_BACKOFF_MS',
      'DRAIN_DROP_AFTER_ATTEMPTS',
      'DRAIN_MAX_DROPS_PER_TICK',
    ]) delete process.env[k];

    expect(getDrainSettings()).toEqual({
      intervalMs: 30_000,
      batchSize: 10,
      baseBackoffMs: 30_000,
      maxBackoffMs: 3_600_000,
      dropAfterAttempts: 3,
      maxDropsPerTick: 5,
    });
  });

  it('parses drain overrides from the environment', () => {
    process.env.DRAIN_INTERVAL_MS = '5000';
    process.env.DRAIN_BATCH_SIZE = '3';
    expect(getDrainSettings().intervalMs).toBe(5000);
    expect(getDrainSettings().batchSize).toBe(3);
    delete process.env.DRAIN_INTERVAL_MS;
    delete process.env.DRAIN_BATCH_SIZE;
  });

  it('falls back to the default when an override is not a positive number', () => {
    process.env.DRAIN_BATCH_SIZE = 'not-a-number';
    expect(getDrainSettings().batchSize).toBe(10);
    delete process.env.DRAIN_BATCH_SIZE;
  });

  it('parses dropAfterAttempts and maxDropsPerTick overrides from the environment', () => {
    process.env.DRAIN_DROP_AFTER_ATTEMPTS = '7';
    process.env.DRAIN_MAX_DROPS_PER_TICK = '2';
    expect(getDrainSettings().dropAfterAttempts).toBe(7);
    expect(getDrainSettings().maxDropsPerTick).toBe(2);
    delete process.env.DRAIN_DROP_AFTER_ATTEMPTS;
    delete process.env.DRAIN_MAX_DROPS_PER_TICK;
  });

  it('falls back to the default when dropAfterAttempts/maxDropsPerTick are not positive numbers', () => {
    process.env.DRAIN_DROP_AFTER_ATTEMPTS = '0';
    process.env.DRAIN_MAX_DROPS_PER_TICK = '-1';
    expect(getDrainSettings().dropAfterAttempts).toBe(3);
    expect(getDrainSettings().maxDropsPerTick).toBe(5);
    delete process.env.DRAIN_DROP_AFTER_ATTEMPTS;
    delete process.env.DRAIN_MAX_DROPS_PER_TICK;
  });

  it('names the dedup db, the WA auth dir, and the health file, so every ensureOutboxDirWritable caller guards the same paths', () => {
    // src/index.ts and scripts/import-export.ts both call
    // ensureOutboxDirWritable(outboxDir, outboxGuards()); if this list ever
    // drops a path, the overlap check silently stops protecting it at BOTH
    // call sites at once. Changing the list is a schema change: you must update
    // this expectation and all call sites.
    const guards = outboxGuards();
    expect(guards).toEqual([
      { label: 'DEDUP_DB', path: getDedupDb() },
      { label: 'WA_AUTH_DIR', path: getWaAuthDir() },
      { label: 'HEALTH_FILE', path: getHealthFile() },
    ]);
  });
});
