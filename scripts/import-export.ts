import { statSync } from 'node:fs';
import { getDedupDb, getOutboxDir, loadConfig, outboxGuards } from '../src/config.ts';
import { logger } from '../src/logger.ts';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import { importFolder } from '../src/sync/importFolder.ts';
import { ensureOutboxDirWritable } from '../src/sync/staging.ts';

/**
 * Bulk-import a WhatsApp "Export chat (with media)" folder into the outbox.
 *
 * Use this for media that predates the bot's group membership (WhatsApp never
 * delivers pre-join history to a member). Export the chat WITH MEDIA from a
 * phone that has the photos, unzip it, then point this script at the folder.
 *
 * This only stages bytes and queues rows — it does not talk to Immich itself.
 * Run the daemon (or its drain loop) afterwards to actually upload; that
 * keeps this script's failure behaviour identical to the live and zip-backfill
 * paths instead of rebuilding a third one.
 *
 * Usage:
 *   npx tsx scripts/import-export.ts <folder> [--album "Album Name"]
 */

function pickAlbumName(): string {
  const flag = process.argv.indexOf('--album');
  if (flag !== -1 && process.argv[flag + 1]) return process.argv[flag + 1];
  const config = loadConfig();
  if (config.albumMode === 'single') return config.singleAlbumName ?? 'WhatsApp';
  if (config.albumMode === 'none') return '';
  const first = config.whitelist[0];
  return first && !first.includes('@') ? first : 'WhatsApp Import';
}

async function main(): Promise<void> {
  const folder = process.argv[2];
  if (!folder || folder.startsWith('--')) {
    logger.error('Usage: npx tsx scripts/import-export.ts <folder> [--album "Name"]');
    process.exit(1);
  }
  statSync(folder); // throws if missing

  const albumName = pickAlbumName();
  const outboxDir = getOutboxDir();
  // Same overlap guard the daemon enforces at startup (src/index.ts) — reused
  // from config.ts so this script and the daemon cannot drift apart. Without
  // it, an OUTBOX_DIR misconfigured to overlap DEDUP_DB or WA_AUTH_DIR (e.g.
  // OUTBOX_DIR=./data) would plant the outbox marker and stage media right
  // next to synced.db/auth/ here, then have the daemon refuse to boot on it.
  await ensureOutboxDirWritable(outboxDir, outboxGuards());
  const db = openDb(getDedupDb());
  // DedupStore before OutboxStore: OutboxStore's constructor assumes the
  // `synced` table (created by DedupStore) already exists on this connection.
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);

  logger.info({ folder, album: albumName || '(none)' }, 'import starting');
  try {
    const stats = await importFolder(folder, { outbox, dedup, outboxDir, albumName, logger });
    logger.info(stats, 'import queued; run the daemon to upload to Immich');
  } finally {
    // Must run even if importFolder throws, or a failed run leaks the sqlite
    // handle (and its WAL lock) instead of closing cleanly.
    dedup.close();
  }
}

main().catch((err) => {
  logger.error(err, 'import-export failed');
  process.exit(1);
});
