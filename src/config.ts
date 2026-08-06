import { z } from 'zod';
import type { AppConfig } from './types.ts';

let dotenvLoaded = false;
function ensureDotenv(): void {
  if (dotenvLoaded) return;
  // Under Vitest, never read the on-disk .env — tests set process.env
  // explicitly and a real .env would clobber that and break isolation.
  if (process.env.VITEST) {
    dotenvLoaded = true;
    return;
  }
  try {
    process.loadEnvFile();
  } catch {
    // no .env file — rely on the real environment
  }
  dotenvLoaded = true;
}

/** Split a comma-separated env value into trimmed, non-empty entries. */
function splitList(v: string | undefined): string[] {
  return (v ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
}

/** Coerce a string env flag to boolean, falling back to `dflt` when unset. */
function parseBool(v: string | undefined, dflt: boolean): boolean {
  if (v === undefined || v.trim() === '') return dflt;
  return /^(1|true|yes|on)$/i.test(v.trim());
}

const appConfigSchema = z.object({
  whitelist: z
    .array(z.string().min(1))
    .min(1, 'WHITELIST_GROUPS must list at least one group name or jid'),
  mediaTypes: z.array(z.enum(['image', 'video'])).min(1, 'MEDIA_TYPES must include image and/or video'),
  backfill: z.boolean(),
  albumMode: z.enum(['per-group', 'single', 'none']),
  singleAlbumName: z.string().optional(),
  backfillGroupName: z.string().min(1),
  reactionEmoji: z.string().min(1).optional(),
});

/** Load and validate sync settings from environment variables (see .env.example). */
export function loadConfig(): AppConfig {
  ensureDotenv();
  return appConfigSchema.parse({
    whitelist: splitList(process.env.WHITELIST_GROUPS),
    mediaTypes: splitList(process.env.MEDIA_TYPES || 'image,video'),
    backfill: parseBool(process.env.BACKFILL, true),
    albumMode: process.env.ALBUM_MODE?.trim() || 'per-group',
    singleAlbumName: process.env.SINGLE_ALBUM_NAME?.trim() || undefined,
    backfillGroupName: process.env.BACKFILL_GROUP_NAME?.trim() || 'wa-immich-backfill',
    reactionEmoji: process.env.SYNC_REACTION_EMOJI?.trim() || undefined,
  });
}

/** Immich connection settings — required for the sync daemon. */
export function loadImmichEnv(): { immichUrl: string; immichApiKey: string } {
  ensureDotenv();
  const schema = z.object({
    IMMICH_URL: z.string().url(),
    IMMICH_API_KEY: z.string().min(1),
  });
  const e = schema.parse(process.env);
  return { immichUrl: e.IMMICH_URL.replace(/\/+$/, ''), immichApiKey: e.IMMICH_API_KEY };
}

/** Directory where Baileys persists multi-device auth state. */
export function getWaAuthDir(): string {
  ensureDotenv();
  return process.env.WA_AUTH_DIR ?? './data/auth';
}

/** Path to the sqlite dedup database. */
export function getDedupDb(): string {
  ensureDotenv();
  return process.env.DEDUP_DB ?? './data/synced.db';
}

/** Read a positive-integer env var, falling back to `dflt` when unset or invalid. */
function intEnv(name: string, dflt: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return dflt;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : dflt;
}

/** Directory where media is staged between WhatsApp capture and Immich upload. */
export function getOutboxDir(): string {
  ensureDotenv();
  return process.env.OUTBOX_DIR ?? './data/outbox';
}

/** Drain loop tuning. Defaults per the Phase 1 design spec and `startDrain`'s own fallbacks. */
export function getDrainSettings(): {
  intervalMs: number;
  batchSize: number;
  baseBackoffMs: number;
  maxBackoffMs: number;
  /** Retries a stuck row earns before its staged file is treated as terminal (see startDrain). */
  dropAfterAttempts: number;
  /** Cap on terminal drops per tick, so a directory outage can't empty the queue. */
  maxDropsPerTick: number;
} {
  ensureDotenv();
  const baseBackoffMs = intEnv('DRAIN_BASE_BACKOFF_MS', 30_000);
  return {
    intervalMs: intEnv('DRAIN_INTERVAL_MS', 30_000),
    batchSize: intEnv('DRAIN_BATCH_SIZE', 10),
    baseBackoffMs,
    // A ceiling below the floor is a misconfiguration, not an intent to retry
    // fast: backoffDelayMs clamps to maxMs, so it would silently turn
    // exponential backoff into a fixed delay. Raise it to the floor instead.
    maxBackoffMs: Math.max(baseBackoffMs, intEnv('DRAIN_MAX_BACKOFF_MS', 3_600_000)),
    dropAfterAttempts: intEnv('DRAIN_DROP_AFTER_ATTEMPTS', 3),
    maxDropsPerTick: intEnv('DRAIN_MAX_DROPS_PER_TICK', 5),
  };
}
