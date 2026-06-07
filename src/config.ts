import { z } from 'zod';
import type { AppConfig } from './types.ts';

let dotenvLoaded = false;
function ensureDotenv(): void {
  if (dotenvLoaded) return;
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
