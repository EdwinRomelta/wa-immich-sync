import { rm } from 'node:fs/promises';
import type { WAMessage, WASocket } from '@whiskeysockets/baileys';
import type { AppConfig, GroupConfig } from '../types.ts';
import { extractMedia, type ExtractDeps } from '../wa/mediaExtractor.ts';
import type { DedupStore } from './dedupStore.ts';
import type { OutboxStore } from './outboxStore.ts';
import { stageFile } from './staging.ts';

type IngestLogger = {
  info: (...a: unknown[]) => void;
  warn: (...a: unknown[]) => void;
  error: (...a: unknown[]) => void;
  debug?: (...a: unknown[]) => void;
};

export interface IngestDeps {
  config: AppConfig;
  dedup: Pick<DedupStore, 'has'>;
  outbox: Pick<OutboxStore, 'has' | 'enqueue'>;
  /** Directory staged media is written to. */
  outboxDir: string;
  logger: IngestLogger;
  /** Injectable extractor for tests. */
  extract?: typeof extractMedia;
  extractDeps?: ExtractDeps;
}

export type IngestOutcome =
  | 'skipped-not-whitelisted'
  | 'skipped-no-media'
  | 'skipped-dedup'
  | 'queued'
  | 'error';

type IngestSock = Pick<WASocket, 'updateMediaMessage' | 'sendMessage'>;

/**
 * Capture a WhatsApp message: whitelist, dedup, extract, write bytes to disk,
 * queue it. Deliberately knows nothing about Immich — once this returns
 * 'queued', the media survives an Immich outage, a crash, and a restart.
 */
export function createIngest(deps: IngestDeps) {
  let whitelist = new Map<string, GroupConfig>();
  const extract = deps.extract ?? extractMedia;

  function setGroups(groups: GroupConfig[]): void {
    whitelist = new Map(groups.map((g) => [g.jid, g]));
  }

  function albumNameFor(group: GroupConfig): string {
    switch (deps.config.albumMode) {
      case 'per-group':
        return group.name;
      case 'single':
        return deps.config.singleAlbumName ?? 'WhatsApp';
      case 'none':
        return '';
    }
  }

  /** True when this message is already recorded, synced or still queued. */
  function known(messageId: string): boolean {
    return deps.dedup.has(messageId) || deps.outbox.has(messageId);
  }

  async function ingest(sock: IngestSock, m: WAMessage): Promise<IngestOutcome> {
    const jid = m.key?.remoteJid ?? '';
    const group = whitelist.get(jid);
    if (!group) return 'skipped-not-whitelisted';

    // Dedup BEFORE downloading. History/append batches replay already-handled
    // messages; downloading them first wastes bandwidth and hammers WhatsApp.
    const rawId = m.key?.id ?? '';
    if (rawId && known(`${jid}:${rawId}`)) {
      deps.logger.debug?.({ messageId: `${jid}:${rawId}` }, 'dedup skip (pre-download)');
      return 'skipped-dedup';
    }

    const item = await extract(sock, m, deps.config, group.name, deps.extractDeps);
    if (!item) {
      // Surface WHAT was skipped — silent drops of unsupported media (e.g.
      // images sent as documents) are otherwise indistinguishable from text.
      deps.logger.info(
        {
          messageId: `${jid}:${rawId}`,
          group: group.name,
          contentKeys: Object.keys((m.message ?? {}) as Record<string, unknown>),
          messageTimestamp: Number(m.messageTimestamp ?? 0),
          stubType: m.messageStubType ?? null,
          hasMessage: m.message != null,
        },
        'skipped-no-media',
      );
      return 'skipped-no-media';
    }

    // Re-check after the download: a concurrent upsert ('notify' then 'append'
    // carrying the same message) may have queued it while we awaited the bytes.
    if (known(item.messageId)) {
      deps.logger.debug?.({ messageId: item.messageId }, 'dedup skip');
      return 'skipped-dedup';
    }

    try {
      // Bytes to disk FIRST, row second. A crash between the two leaves an
      // orphan file (swept at startup), never a row without its media.
      const filePath = await stageFile(deps.outboxDir, item.messageId, item.buffer);
      try {
        deps.outbox.enqueue({
          messageId: item.messageId,
          groupJid: item.groupJid,
          albumName: albumNameFor(group),
          filePath,
          fileName: item.fileName,
          mimeType: item.mimeType,
          capturedAt: item.timestamp.getTime(),
          createdAt: Date.now(),
        });
      } catch (err) {
        // The row never landed, so nothing will ever reference these bytes.
        // Drop them now rather than leaving an orphan for the startup sweep —
        // this daemon is meant to run unattended for weeks.
        await rm(filePath, { force: true }).catch(() => {});
        throw err;
      }
      deps.logger.info({ messageId: item.messageId, group: group.name, kind: item.kind }, 'queued');
    } catch (err) {
      // Pass the whole error so pino's serializer keeps the type and stack.
      deps.logger.error(
        { err, code: (err as NodeJS.ErrnoException).code },
        `ingest failed for ${item.messageId}`,
      );
      return 'error';
    }

    // Mark the message in WhatsApp. This now means "captured safely", not
    // "already in Immich" — drain runs later and holds no socket. A failed
    // reaction must not change the outcome: the media is already durable.
    if (deps.config.reactionEmoji && m.key) {
      try {
        await sock.sendMessage(jid, { react: { text: deps.config.reactionEmoji, key: m.key } });
      } catch (err) {
        deps.logger.warn(
          { messageId: item.messageId, err: (err as Error).message },
          'reaction failed',
        );
      }
    }

    return 'queued';
  }

  return { ingest, setGroups };
}
