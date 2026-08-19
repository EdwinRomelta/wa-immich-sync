import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  getAlertSettings,
  getDedupDb,
  getDrainSettings,
  getHealthFile,
  getHealthMonitorSettings,
  getHealthSettings,
  getOutboxDir,
  getWaAuthDir,
  loadConfig,
  outboxGuards,
} from '../src/config.ts';

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

describe('health settings', () => {
  it('uses the documented defaults', () => {
    delete process.env.HEALTH_STALE_MS;
    delete process.env.HEALTH_INTERVAL_MS;
    expect(getHealthSettings()).toEqual({ staleMs: 3_600_000 });
    expect(getHealthMonitorSettings()).toEqual({ intervalMs: 60_000 });
  });

  it('parses overrides from the environment', () => {
    process.env.HEALTH_STALE_MS = '7200000';
    process.env.HEALTH_INTERVAL_MS = '30000';
    expect(getHealthSettings()).toEqual({ staleMs: 7_200_000 });
    expect(getHealthMonitorSettings()).toEqual({ intervalMs: 30_000 });
    delete process.env.HEALTH_STALE_MS;
    delete process.env.HEALTH_INTERVAL_MS;
  });

  it('falls back to the default when an override is not a positive number', () => {
    process.env.HEALTH_STALE_MS = 'not-a-number';
    process.env.HEALTH_INTERVAL_MS = '-5';
    expect(getHealthSettings()).toEqual({ staleMs: 3_600_000 });
    expect(getHealthMonitorSettings()).toEqual({ intervalMs: 60_000 });
    delete process.env.HEALTH_STALE_MS;
    delete process.env.HEALTH_INTERVAL_MS;
  });
});

describe('alert settings', () => {
  const ALERT_KEYS = [
    'ALERT_TARGET_JID',
    'ALERT_COOLDOWN_MS',
    'ALERT_OUTBOX_DEPTH',
    'ALERT_OUTBOX_AGE_MS',
    'ALERT_RECONNECT_FAILURES',
    'CATCHUP_GAP_THRESHOLD_MS',
  ];

  afterEach(() => {
    for (const k of ALERT_KEYS) delete process.env[k];
  });

  it('uses the documented defaults, with targetJid undefined (falls back to the bot\'s own number)', () => {
    for (const k of ALERT_KEYS) delete process.env[k];
    expect(getAlertSettings()).toEqual({
      cooldownMs: 21_600_000,
      targetJid: undefined,
      outboxDepth: 50,
      outboxAgeMs: 7_200_000,
      reconnectFailures: 10,
      gapThresholdMs: 3_600_000,
    });
  });

  it('parses overrides from the environment', () => {
    process.env.ALERT_TARGET_JID = '628123456789@s.whatsapp.net';
    process.env.ALERT_COOLDOWN_MS = '1000';
    process.env.ALERT_OUTBOX_DEPTH = '5';
    process.env.ALERT_OUTBOX_AGE_MS = '2000';
    process.env.ALERT_RECONNECT_FAILURES = '3';
    process.env.CATCHUP_GAP_THRESHOLD_MS = '4000';
    expect(getAlertSettings()).toEqual({
      cooldownMs: 1000,
      targetJid: '628123456789@s.whatsapp.net',
      outboxDepth: 5,
      outboxAgeMs: 2000,
      reconnectFailures: 3,
      gapThresholdMs: 4000,
    });
  });

  it('falls back to defaults when numeric overrides are not positive numbers', () => {
    process.env.ALERT_COOLDOWN_MS = 'not-a-number';
    process.env.ALERT_OUTBOX_DEPTH = '0';
    process.env.ALERT_RECONNECT_FAILURES = '-1';
    const s = getAlertSettings();
    expect(s.cooldownMs).toBe(21_600_000);
    expect(s.outboxDepth).toBe(50);
    expect(s.reconnectFailures).toBe(10);
  });

  it('treats an unset ALERT_TARGET_JID as undefined', () => {
    delete process.env.ALERT_TARGET_JID;
    expect(getAlertSettings().targetJid).toBeUndefined();
  });

  it('treats an empty-string ALERT_TARGET_JID as undefined, not as a literal empty JID', () => {
    // The critical case: src/alert/alerter.ts does
    // `deps.targetJid ?? selfJid(...)`, and `??` does NOT treat '' as
    // nullish. If a blank env var ever leaked through as '', every alert
    // would silently try to send to '' instead of falling back to the bot's
    // own number — the alert channel would go dark with no error anywhere.
    process.env.ALERT_TARGET_JID = '';
    expect(getAlertSettings().targetJid).toBeUndefined();
  });

  it('treats a whitespace-only ALERT_TARGET_JID as undefined', () => {
    process.env.ALERT_TARGET_JID = '   ';
    expect(getAlertSettings().targetJid).toBeUndefined();
  });

  it('trims a valid ALERT_TARGET_JID', () => {
    process.env.ALERT_TARGET_JID = '  628123456789@s.whatsapp.net  ';
    expect(getAlertSettings().targetJid).toBe('628123456789@s.whatsapp.net');
  });
});
