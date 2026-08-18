import type { WAMessage, WASocket } from '@whiskeysockets/baileys';
import {
  getAlertSettings,
  getDedupDb,
  getDrainSettings,
  getHealthFile,
  getHealthMonitorSettings,
  getOutboxDir,
  getWaAuthDir,
  loadConfig,
  loadImmichEnv,
  outboxGuards,
} from './config.ts';
import { logger } from './logger.ts';
import { ImmichClient } from './immich/client.ts';
import { openDb } from './sync/db.ts';
import { DedupStore } from './sync/dedupStore.ts';
import { OutboxStore } from './sync/outboxStore.ts';
import { createIngest } from './sync/ingest.ts';
import { startDrain } from './sync/drain.ts';
import { ensureOutboxDirWritable, sweepOrphans } from './sync/staging.ts';
import { OldestAnchors, startBackfill } from './sync/backfill.ts';
import { handleBackfillMessage } from './sync/backfillIngest.ts';
import { resolveWhitelist } from './wa/groupResolver.ts';
import { startWaClient } from './wa/client.ts';
import { createGate } from './util/gate.ts';
import { AlertStore } from './alert/alertStore.ts';
import { createAlerter } from './alert/alerter.ts';
import { startHealthMonitor } from './health/monitor.ts';
import { describeGap, detectGaps, lastCapturedByGroup } from './sync/gapDetect.ts';

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

  // No Immich readiness gate: ingest stages media to disk regardless of
  // whether Immich is reachable, and drain retries with backoff until it
  // answers. Blocking startup on `immich.ping()` used to abort the whole
  // process after ~20 minutes of Immich downtime — a crash mode that only
  // existed because an in-flight message had nowhere safe to go. With the
  // outbox it does, so WhatsApp connects immediately regardless of Immich.
  const outboxDir = getOutboxDir();
  const dedupDb = getDedupDb();
  // The startup sweep below deletes every regular file it doesn't recognise
  // from outboxDir. If OUTBOX_DIR is ever misconfigured to overlap the dedup
  // db file or the WhatsApp auth dir — a one-character `.env` edit, e.g.
  // OUTBOX_DIR=./data — that sweep deletes synced.db or creds.json instead of
  // orphaned staged media. Refuse to start rather than risk it. Guarded with
  // the dedup db's own file path, not its parent directory: the shipped
  // defaults (OUTBOX_DIR=./data/outbox, DEDUP_DB=./data/synced.db,
  // WA_AUTH_DIR=./data/auth) are siblings under ./data by design, and
  // guarding on dirname(dedupDb) would flag that entirely safe default as an
  // overlap. The guard list itself lives in outboxGuards() (src/config.ts) so
  // this daemon and scripts/import-export.ts — the other caller of
  // ensureOutboxDirWritable — cannot drift apart and silently drop the check.
  await ensureOutboxDirWritable(outboxDir, outboxGuards());

  const db = openDb(dedupDb);
  // DedupStore must be constructed before OutboxStore on this shared
  // connection: OutboxStore.markSyncedAndRemove writes to the `synced` table
  // that DedupStore's constructor creates.
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  const alertStore = new AlertStore(db);

  // A crash between staging a file and inserting its row leaves an orphan.
  const swept = await sweepOrphans(outboxDir, outbox.allFilePaths());
  if (swept > 0) logger.info({ swept }, 'outbox: removed orphaned staged files');

  // The socket is replaced on every reconnect, so the alerter reads it through
  // a getter rather than capturing one instance.
  let currentSock: WASocket | null = null;
  // Last WhatsApp activity, in-memory. The monitor copies it into the
  // heartbeat file once per tick; stamping the file per message would mean an
  // fsync-adjacent write on every photo for no extra signal.
  let lastWaActivityAt: number | null = null;
  const noteWaActivity = (): void => {
    lastWaActivityAt = Date.now();
  };

  const alertSettings = getAlertSettings();
  const alerter = createAlerter({
    store: alertStore,
    getSock: () => currentSock,
    targetJid: alertSettings.targetJid,
    cooldownMs: alertSettings.cooldownMs,
    logger,
  });

  const ingest = createIngest({
    config,
    dedup,
    outbox,
    outboxDir,
    logger,
    extractDeps: { logger },
    onCaptureFailed: ({ messageId, groupJid, error }) => {
      // Fire-and-forget: ingest is on the message hot path and must not wait
      // on a WhatsApp round-trip. raise() never throws, but .catch anyway —
      // an unhandled rejection here would kill the process under
      // `restart: always`.
      void alerter
        .raise(
          'capture-failed',
          `wa-immich-sync: could not capture media from ${groupJid} (${messageId}). ` +
            `This one is NOT queued and will not retry. Error: ${error}`,
        )
        .catch((err) => logger.warn({ err: (err as Error).message }, 'capture-failed alert threw'));
    },
  });
  const drainSettings = getDrainSettings();
  const drain = startDrain({ immich, outbox, logger, ...drainSettings });
  logger.info({ ...drainSettings, outboxDir, pending: outbox.depth() }, 'drain started');
  // Run a first pass immediately instead of waiting out the full
  // DRAIN_INTERVAL_MS (30s default): otherwise a backlog built up while the
  // process was down sits idle after every boot for no reason. tick()'s
  // re-entrancy guard makes an overlapping call from the timer loop safe,
  // but its returned promise CAN reject (synchronous better-sqlite3 calls
  // sit outside its own try/catch) — left uncaught, that becomes an
  // unhandled rejection that kills the process, and with `restart: always`
  // in docker-compose.yml, a crash loop that also drops the WhatsApp socket.
  void drain.tick().catch((err) => {
    logger.error({ err: (err as Error).message }, 'drain: initial tick failed');
  });

  const healthMonitorSettings = getHealthMonitorSettings();
  const healthFile = getHealthFile();
  const healthMonitor = startHealthMonitor({
    outbox,
    alerter,
    heartbeatPath: healthFile,
    waActivity: () => lastWaActivityAt,
    thresholds: {
      outboxDepth: alertSettings.outboxDepth,
      outboxAgeMs: alertSettings.outboxAgeMs,
    },
    intervalMs: healthMonitorSettings.intervalMs,
    logger,
  });
  logger.info(
    { ...healthMonitorSettings, healthFile, outboxDepth: alertSettings.outboxDepth, outboxAgeMs: alertSettings.outboxAgeMs },
    'health monitor started',
  );
  // Stamp the heartbeat immediately: the Dockerfile's start-period covers the
  // boot window, but a first tick a full interval later leaves the file absent
  // (and therefore "unhealthy") for no reason.
  void healthMonitor.tick().catch((err) => {
    logger.warn({ err: (err as Error).message }, 'health: initial tick failed');
  });

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
    // Stamps `wa` from the raw frame stream (keepalives included), so the
    // healthcheck tracks link liveness rather than chat traffic. onMessage
    // and onReady below still stamp it too — harmless, and it means a fresh
    // message or connection-open counts immediately rather than waiting for
    // the next keepalive.
    onFrame: noteWaActivity,
    onMessage: async (sock, m) => {
      noteWaActivity();
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
          outbox,
          dedup,
          outboxDir,
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
      currentSock = sock;
      noteWaActivity();
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

      // Gap detection. Reports only — Phase 3 adds the catch-up traversal that
      // recovers the window. Runs after the whitelist is resolved so
      // whitelistJids is populated, and before the early return below, so it
      // still runs with BACKFILL=false.
      try {
        const gaps = detectGaps({
          lastKnown: lastCapturedByGroup(db),
          groupJids: [...whitelistJids],
          now: Date.now(),
          thresholdMs: alertSettings.gapThresholdMs,
        });
        for (const gap of gaps) {
          logger.warn({ ...gap }, 'gap detected');
          // void + .catch, not await: raise() can hang on a half-open
          // WhatsApp socket, and awaiting it here would stall onReady for
          // this connection generation — backfill anchor seeding and pump
          // start below would never run. Keyed per group so one quiet group
          // cannot cool down the alert for every other group.
          void alerter
            .raise(
              `gap:${gap.groupJid}`,
              `wa-immich-sync: ${describeGap(gap)}. If the daemon was down, that window ` +
                'is not recovered automatically — re-import a chat export to fill it.',
            )
            .catch((err) => logger.warn({ err: (err as Error).message }, 'gap alert threw'));
        }
      } catch (err) {
        logger.warn({ err: (err as Error).message }, 'gap detection failed');
      }

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
    onReconnectScheduled: ({ attempt, delayMs, statusCode }) => {
      if (attempt < alertSettings.reconnectFailures) return;
      void alerter
        .raise(
          'reconnect-failures',
          `wa-immich-sync: WhatsApp has failed to reconnect ${attempt} times in a row ` +
            `(status ${statusCode ?? 'unknown'}, next try in ${Math.round(delayMs / 1000)}s). ` +
            'Media sent during this window may not be recoverable.',
        )
        .catch((err) => logger.warn({ err: (err as Error).message }, 'reconnect alert threw'));
    },
  });

  // How long a graceful shutdown is given before it's forced. drain.stop()
  // waits out a whole in-flight tick — up to batchSize (10) sequential
  // uploads with no per-row timeout — so an unbounded wait here has no upper
  // bound either. docker-compose.yml sets stop_grace_period well above this
  // so Docker's SIGKILL budget is never the tighter constraint.
  const GRACEFUL_SHUTDOWN_TIMEOUT_MS = 8_000;

  // Signal counter, not a boolean re-entrancy guard: a boolean would make a
  // SECOND SIGTERM/SIGINT arriving mid-shutdown a silent no-op, leaving the
  // operator with no escape hatch while drain.stop() waits out a slow
  // upload — Ctrl-C stops working, and `docker compose restart` just waits
  // out its grace period and SIGKILLs (exit 137, not 0). The first signal
  // still shuts down gracefully; only the second forces an immediate exit.
  let shutdownSignals = 0;
  const shutdown = async (): Promise<void> => {
    shutdownSignals += 1;
    if (shutdownSignals > 1) {
      logger.warn('second shutdown signal received — exiting immediately');
      process.exit(130);
      return;
    }
    logger.info('shutting down');
    healthMonitor.stop();

    // Bounded, not unbounded: race drain.stop() against a timeout so a slow
    // batch can't hang shutdown forever. `.unref()` the timer so it can never
    // itself keep the process alive if drain.stop() wins the race first.
    await new Promise<void>((resolveShutdown) => {
      let settled = false;
      const finish = (): void => {
        if (settled) return;
        settled = true;
        resolveShutdown();
      };
      const timer = setTimeout(() => {
        logger.warn({ timeoutMs: GRACEFUL_SHUTDOWN_TIMEOUT_MS }, 'drain.stop() timed out; closing anyway');
        finish();
      }, GRACEFUL_SHUTDOWN_TIMEOUT_MS);
      timer.unref();
      void drain.stop().finally(() => {
        clearTimeout(timer);
        finish();
      });
    });

    // Must not let a throwing close() skip process.exit(0): an uncaught
    // rejection here would escape `void shutdown()` and, on Node 22, turn a
    // clean shutdown into a non-zero exit — the exact outcome this handler
    // exists to prevent. This also covers the timeout path above, where
    // close() may now run underneath a write drain.stop() never finished
    // waiting for. docker-compose.yml sets `init: true` so this handler gets
    // to run at all when SIGTERM arrives.
    try {
      dedup.close();
    } catch (err) {
      logger.warn({ err: (err as Error).message }, 'dedup.close() failed during shutdown');
    }
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown());
  process.on('SIGTERM', () => void shutdown());
}

main().catch((err) => {
  logger.error(err, 'fatal');
  process.exit(1);
});
