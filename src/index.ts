import { mkdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { WAMessage } from '@whiskeysockets/baileys';
import { getDedupDb, getDrainSettings, getOutboxDir, getWaAuthDir, loadConfig, loadImmichEnv } from './config.ts';
import { logger } from './logger.ts';
import { ImmichClient } from './immich/client.ts';
import { openDb } from './sync/db.ts';
import { DedupStore } from './sync/dedupStore.ts';
import { OutboxStore } from './sync/outboxStore.ts';
import { createIngest } from './sync/ingest.ts';
import { startDrain } from './sync/drain.ts';
import { sweepOrphans } from './sync/staging.ts';
import { OldestAnchors, startBackfill } from './sync/backfill.ts';
import { handleBackfillMessage } from './sync/backfillIngest.ts';
import { resolveWhitelist } from './wa/groupResolver.ts';
import { startWaClient } from './wa/client.ts';
import { createGate } from './util/gate.ts';

/** WhatsApp message timestamp (seconds), tolerant of number | Long | undefined. */
function tsSecOf(m: WAMessage): number {
  const raw = m.messageTimestamp;
  return typeof raw === 'number' ? raw : Number(raw ?? 0);
}

/**
 * Create the outbox staging directory and confirm it is actually writable.
 *
 * Without this, a misconfigured OUTBOX_DIR (bad permissions, a bind mount not
 * ready yet, a typo'd path) makes ingest fail silently and repeatedly: every
 * single WhatsApp message returns 'error' forever, one log line at a time,
 * while traffic streams past unrecorded. This is the one startup check that
 * SHOULD block — unlike the Immich readiness gate removed below, there is no
 * safe way to proceed without a writable place to put bytes.
 */
async function ensureOutboxDirWritable(dir: string): Promise<void> {
  await mkdir(dir, { recursive: true });
  const probePath = join(dir, '.write-probe');
  await writeFile(probePath, '');
  await rm(probePath, { force: true });
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

  // No Immich readiness gate: ingest stages media to disk regardless of
  // whether Immich is reachable, and drain retries with backoff until it
  // answers. Blocking startup on `immich.ping()` used to abort the whole
  // process after ~20 minutes of Immich downtime — a crash mode that only
  // existed because an in-flight message had nowhere safe to go. With the
  // outbox it does, so WhatsApp connects immediately regardless of Immich.
  const outboxDir = getOutboxDir();
  await ensureOutboxDirWritable(outboxDir);

  const db = openDb(getDedupDb());
  // DedupStore must be constructed before OutboxStore on this shared
  // connection: OutboxStore.markSyncedAndRemove writes to the `synced` table
  // that DedupStore's constructor creates.
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);

  // A crash between staging a file and inserting its row leaves an orphan.
  const swept = await sweepOrphans(outboxDir, outbox.allFilePaths());
  if (swept > 0) logger.info({ swept }, 'outbox: removed orphaned staged files');

  const ingest = createIngest({ config, dedup, outbox, outboxDir, logger, extractDeps: { logger } });
  const drainSettings = getDrainSettings();
  const drain = startDrain({ immich, outbox, logger, ...drainSettings });
  logger.info({ ...drainSettings, outboxDir, pending: outbox.depth() }, 'drain started');

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

  // WhatsApp can deliver the initial history batch ~200ms after connect —
  // before groupFetchAllParticipating() resolves the whitelist. Processing
  // against an empty whitelist drops real messages as "not whitelisted"
  // (photos queued while the host was off land exactly in that batch). Hold
  // all message processing until the first whitelist resolution completes.
  const whitelistGate = createGate();

  await startWaClient({
    authDir: getWaAuthDir(),
    syncFullHistory: config.backfill,
    logger,
    onMessage: async (sock, m) => {
      await whitelistGate.wait();
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
        const outcome = await ingest.ingest(sock, m);
        if (outcome === 'queued') logger.info({ jid }, 'live queued');
      } catch (err) {
        logger.warn({ err: (err as Error).message }, 'live process threw');
      }
    },
    onHistory: config.backfill
      ? async (sock, messages) => {
          await whitelistGate.wait();
          const batchTally: Record<string, number> = {};
          for (const m of messages) {
            noteAnchor(m);
            try {
              const o = await ingest.ingest(sock, m);
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
        ingest.setGroups(resolved);
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

      // Open even if resolution failed — buffered messages then evaluate
      // against the previous (or empty) whitelist, same as before this gate.
      whitelistGate.open();

      if (!config.backfill) return;

      // Seed the backfill cursor from the dedup DB. WhatsApp's on-connect
      // history sync times out in some links (no messaging-history.set ever
      // fires), leaving anchors empty so the pump never pages. Seeding from the
      // newest known message lets on-demand fetchMessageHistory page backward
      // and recover gaps without re-pairing.
      for (const seed of dedup.newestByGroup()) {
        if (!whitelistJids.has(seed.group_jid)) continue;
        anchors.note(
          seed.group_jid,
          { remoteJid: seed.group_jid, id: seed.raw_id, fromMe: false },
          Math.floor(seed.created_at / 1000),
        );
        logger.info({ jid: seed.group_jid, anchor: seed.raw_id }, 'backfill: seeded from dedup db');
      }

      // Restart the pump on each (re)connect with the live socket; anchors persist.
      backfill?.stop();
      backfill = startBackfill({ sock, groupJids: [...whitelistJids], anchors, logger });
      logger.info({ groups: whitelistJids.size }, 'backfill: pump started');
    },
  });

  // Re-entrancy guard: a second SIGTERM/SIGINT arriving while shutdown is
  // already in flight must not race a second drain.stop()/dedup.close() pair
  // against the first. The flag is set synchronously before the first
  // `await`, so a signal that arrives after that point sees it immediately.
  let shuttingDown = false;
  const shutdown = async (): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info('shutting down');
    // Must be awaited: drain.stop() waits for any in-flight tick to finish
    // before returning. Closing the shared sqlite handle underneath a live
    // markSyncedAndRemove/defer call throws inside a timer callback and would
    // exit the container with code 1 instead of 0 — docker-compose.yml sets
    // `init: true` specifically so this handler can close sqlite cleanly.
    await drain.stop();
    dedup.close();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown());
  process.on('SIGTERM', () => void shutdown());
}

main().catch((err) => {
  logger.error(err, 'fatal');
  process.exit(1);
});
