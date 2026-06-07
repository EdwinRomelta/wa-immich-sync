import type { WAMessage, WASocket } from '@whiskeysockets/baileys';
import type { AppConfig, GroupConfig } from '../types.ts';
import { extractMedia, type ExtractDeps } from '../wa/mediaExtractor.ts';
import type { ImmichClient } from '../immich/client.ts';
import type { DedupStore } from './dedupStore.ts';

type PipelineLogger = {
  info: (...a: unknown[]) => void;
  warn: (...a: unknown[]) => void;
  error: (...a: unknown[]) => void;
  debug?: (...a: unknown[]) => void;
};

export interface PipelineDeps {
  config: AppConfig;
  immich: Pick<ImmichClient, 'uploadAsset' | 'ensureAlbum' | 'addToAlbum'>;
  dedup: Pick<DedupStore, 'has' | 'markDone'>;
  logger: PipelineLogger;
  /** Injectable extractor for tests. */
  extract?: typeof extractMedia;
  extractDeps?: ExtractDeps;
}

export type ProcessOutcome =
  | 'skipped-not-whitelisted'
  | 'skipped-no-media'
  | 'skipped-dedup'
  | 'uploaded'
  | 'error';

type DownloadSock = Pick<WASocket, 'updateMediaMessage' | 'sendMessage'>;

/** Wire the whitelist → extract → dedup → upload → album → mark-done flow. */
export function createPipeline(deps: PipelineDeps) {
  // Whitelist is set at runtime once group names/jids are resolved (see setGroups).
  let whitelist = new Map<string, GroupConfig>();
  const extract = deps.extract ?? extractMedia;

  /** Replace the set of synced groups (called after whitelist resolution). */
  function setGroups(groups: GroupConfig[]): void {
    whitelist = new Map(groups.map((g) => [g.jid, g]));
  }

  function albumNameFor(group: GroupConfig): string | null {
    switch (deps.config.albumMode) {
      case 'per-group':
        return group.name;
      case 'single':
        return deps.config.singleAlbumName ?? 'WhatsApp';
      case 'none':
        return null;
    }
  }

  async function process(sock: DownloadSock, m: WAMessage): Promise<ProcessOutcome> {
    const jid = m.key?.remoteJid ?? '';
    const group = whitelist.get(jid);
    if (!group) return 'skipped-not-whitelisted';

    const item = await extract(sock, m, deps.config, group.name, deps.extractDeps);
    if (!item) return 'skipped-no-media';

    if (deps.dedup.has(item.messageId)) {
      deps.logger.debug?.({ messageId: item.messageId }, 'dedup skip');
      return 'skipped-dedup';
    }

    try {
      const uploaded = await deps.immich.uploadAsset(item);

      const albumName = albumNameFor(group);
      if (albumName) {
        const albumId = await deps.immich.ensureAlbum(albumName);
        await deps.immich.addToAlbum(albumId, uploaded.assetId);
      }

      deps.dedup.markDone(item.messageId, item.groupJid, uploaded.assetId, uploaded.status);
      deps.logger.info(
        { group: group.name, kind: item.kind, status: uploaded.status, assetId: uploaded.assetId },
        'synced',
      );

      // Mark the message in WhatsApp so it's visibly "already synced". A failed
      // reaction must not change the outcome — the media is already in Immich.
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

      return 'uploaded';
    } catch (err) {
      // Leave the message unmarked so it retries on the next run.
      deps.logger.error(err, `sync failed for ${item.messageId}`);
      return 'error';
    }
  }

  return { process, setGroups };
}
