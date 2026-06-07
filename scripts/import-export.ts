import { statSync } from 'node:fs';
import { getDedupDb, loadConfig, loadImmichEnv } from '../src/config.ts';
import { logger } from '../src/logger.ts';
import { ImmichClient } from '../src/immich/client.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { importFolder } from '../src/sync/importFolder.ts';

/**
 * Bulk-import a WhatsApp "Export chat (with media)" folder into Immich.
 *
 * Use this for media that predates the bot's group membership (WhatsApp never
 * delivers pre-join history to a member). Export the chat WITH MEDIA from a
 * phone that has the photos, unzip it, then point this script at the folder.
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

  const { immichUrl, immichApiKey } = loadImmichEnv();
  const albumName = pickAlbumName();
  const immich = new ImmichClient({ baseUrl: immichUrl, apiKey: immichApiKey });
  const dedup = new DedupStore(getDedupDb());

  logger.info({ folder, album: albumName || '(none)' }, 'import starting');
  const stats = await importFolder(folder, { immich, dedup, albumName, logger });
  dedup.close();
  logger.info(stats, 'import complete');
}

main().catch((err) => {
  logger.error(err, 'import-export failed');
  process.exit(1);
});
