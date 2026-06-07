import type { WAMessage } from '@whiskeysockets/baileys';
import { getDedupDb, getWaAuthDir, loadConfig, loadImmichEnv } from './config.ts';
import { logger } from './logger.ts';
import { ImmichClient } from './immich/client.ts';
import { DedupStore } from './sync/dedupStore.ts';
import { createPipeline } from './sync/pipeline.ts';
import { OldestAnchors, startBackfill } from './sync/backfill.ts';
import { handleBackfillMessage } from './sync/backfillIngest.ts';
import { resolveWhitelist } from './wa/groupResolver.ts';
import { startWaClient } from './wa/client.ts';

/** WhatsApp message timestamp (seconds), tolerant of number | Long | undefined. */
function tsSecOf(m: WAMessage): number {
  const raw = m.messageTimestamp;
  return typeof raw === 'number' ? raw : Number(raw ?? 0);
}

async function main(): Promise<void> {
  const config = loadConfig();
  const { immichUrl, immichApiKey } = loadImmichEnv();

  logger.info(
    {
      whitelist: config.whitelist,
      backfill: config.backfill,
      albumMode: config.albumMode,
      backfillGroup: config.backfillGroupName,
    },
    'wa-immich-sync starting',
  );

  const immich = new ImmichClient({ baseUrl: immichUrl, apiKey: immichApiKey });
  const dedup = new DedupStore(getDedupDb());
  const pipeline = createPipeline({ config, immich, dedup, logger, extractDeps: { logger } });

  // Backfill cursor: oldest seen message per whitelisted group, fed by both
  // history and live messages, paged backwards via fetchMessageHistory. The
  // whitelist jids are filled in once group names/jids are resolved on connect.
  const whitelistJids = new Set<string>();
  const anchors = new OldestAnchors();
  const noteAnchor = (m: WAMessage) => {
    const jid = m.key?.remoteJid ?? '';
    if (whitelistJids.has(jid)) anchors.note(jid, m.key, tsSecOf(m));
  };
  let backfill: { stop: () => void } | null = null;

  // Dedicated group where exported-chat .zip archives are imported, resolved by
  // name on connect (and lazily when a document arrives from an unknown group).
  let backfillGroupJid: string | null = null;
  let backfillDefaultAlbum = config.singleAlbumName ?? 'WhatsApp Backfill';

  await startWaClient({
    authDir: getWaAuthDir(),
    syncFullHistory: config.backfill,
    logger,
    onMessage: async (sock, m) => {
      const jid = m.key?.remoteJid ?? '';
      const hasDocument = JSON.stringify(m.message ?? {}).includes('documentMessage');

      // Lazily identify the backfill group by name when a document shows up from
      // an unknown group (handles the group being created after startup).
      if (hasDocument && jid.endsWith('@g.us') && jid !== backfillGroupJid && !whitelistJids.has(jid)) {
        try {
          const meta = await sock.groupMetadata(jid);
          if (meta.subject === config.backfillGroupName) {
            backfillGroupJid = jid;
            logger.info({ jid }, 'backfill group resolved (lazy)');
          }
        } catch {
          // not resolvable — ignore
        }
      }

      // Zip archives dropped in the dedicated backfill group are imported, not
      // treated as normal media.
      if (backfillGroupJid && jid === backfillGroupJid) {
        const handled = await handleBackfillMessage(sock, m, {
          immich,
          dedup,
          logger,
          defaultAlbum: backfillDefaultAlbum,
        });
        if (handled) return;
      }

      noteAnchor(m);
      try {
        const outcome = await pipeline.process(sock, m);
        if (outcome === 'uploaded') logger.info({ jid }, 'live upload');
      } catch (err) {
        logger.warn({ err: (err as Error).message }, 'live process threw');
      }
    },
    onHistory: config.backfill
      ? async (sock, messages) => {
          const batchTally: Record<string, number> = {};
          for (const m of messages) {
            noteAnchor(m);
            try {
              const o = await pipeline.process(sock, m);
              batchTally[o] = (batchTally[o] ?? 0) + 1;
            } catch (err) {
              batchTally.throw = (batchTally.throw ?? 0) + 1;
              logger.warn({ err: (err as Error).message }, 'history process threw');
            }
          }
          logger.info({ count: messages.length, batchTally }, 'history batch processed');
        }
      : undefined,
    onReady: async (sock) => {
      logger.info('ready — resolving groups');

      try {
        const groupsMap = await sock.groupFetchAllParticipating();
        const groups = Object.values(groupsMap).map((g) => ({ id: g.id, subject: g.subject }));

        // Whitelist (names or jids) → concrete groups.
        const { resolved, warnings } = resolveWhitelist(groups, config.whitelist);
        for (const w of warnings) logger.warn({ warning: w }, 'whitelist');
        pipeline.setGroups(resolved);
        whitelistJids.clear();
        for (const g of resolved) whitelistJids.add(g.jid);
        if (resolved[0]) backfillDefaultAlbum = resolved[0].name;
        logger.info(
          { count: resolved.length, groups: resolved.map((g) => g.name) },
          'whitelist resolved',
        );

        // Dedicated backfill group.
        const bf = groups.find((g) => g.subject === config.backfillGroupName);
        backfillGroupJid = bf?.id ?? null;
        logger.info(
          { name: config.backfillGroupName, jid: backfillGroupJid ?? '(not found)' },
          'backfill group resolved',
        );
      } catch (err) {
        logger.warn({ err: (err as Error).message }, 'group resolution failed');
      }

      if (!config.backfill) return;
      // Restart the pump on each (re)connect with the live socket; anchors persist.
      backfill?.stop();
      backfill = startBackfill({ sock, groupJids: [...whitelistJids], anchors, logger });
      logger.info({ groups: whitelistJids.size }, 'backfill: pump started');
    },
  });

  const shutdown = () => {
    logger.info('shutting down');
    dedup.close();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  logger.error(err, 'fatal');
  process.exit(1);
});
