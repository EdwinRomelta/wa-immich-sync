import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import AdmZip from 'adm-zip';
import { downloadMediaMessage, type WAMessage, type WASocket } from '@whiskeysockets/baileys';
import type { ImmichClient } from '../immich/client.ts';
import type { DedupStore } from './dedupStore.ts';
import { importFolder } from './importFolder.ts';

type AnyMsg = Record<string, any> | null | undefined;

type IngestLogger = {
  info: (...a: unknown[]) => void;
  warn: (...a: unknown[]) => void;
  error: (...a: unknown[]) => void;
};

export interface BackfillIngestDeps {
  immich: Pick<ImmichClient, 'uploadAsset' | 'ensureAlbum' | 'addToAlbum'>;
  dedup: Pick<DedupStore, 'has' | 'markDone'>;
  logger: IngestLogger;
  /** Album used when a zip arrives with no caption. */
  defaultAlbum: string;
  /** Injectable downloader for tests. */
  download?: typeof downloadMediaMessage;
}

/** Locate a documentMessage through the common WhatsApp wrappers. */
function findDocument(message: AnyMsg): { doc: AnyMsg; caption?: string } | null {
  if (!message) return null;
  const candidates = [
    message.documentMessage,
    message.documentWithCaptionMessage?.message?.documentMessage,
    message.ephemeralMessage?.message?.documentMessage,
    message.ephemeralMessage?.message?.documentWithCaptionMessage?.message?.documentMessage,
    message.viewOnceMessageV2?.message?.documentMessage,
  ];
  for (const doc of candidates) {
    if (doc) return { doc, caption: doc.caption || undefined };
  }
  return null;
}

function isZip(doc: NonNullable<AnyMsg>): boolean {
  const name: string = doc.fileName ?? '';
  const mime: string = doc.mimetype ?? '';
  return name.toLowerCase().endsWith('.zip') || mime.includes('zip');
}

type DownloadSock = Pick<WASocket, 'updateMediaMessage' | 'sendMessage'>;

/**
 * Handle a message in the dedicated backfill group. If it carries a `.zip`
 * document (a WhatsApp "Export chat with media" archive), download it, unzip
 * in a temp dir, import every photo/video to Immich, and reply with a summary.
 *
 * Returns true if the message was a zip handled here, false otherwise.
 */
export async function handleBackfillMessage(
  sock: DownloadSock,
  m: WAMessage,
  deps: BackfillIngestDeps,
): Promise<boolean> {
  const jid = m.key?.remoteJid ?? '';
  const found = findDocument(m.message as AnyMsg);
  if (!found?.doc || !isZip(found.doc)) return false;

  const albumName = found.caption?.trim() || deps.defaultAlbum;
  const fileName: string = found.doc.fileName ?? 'archive.zip';
  deps.logger.info({ fileName, album: albumName }, 'backfill: zip received, downloading');

  const tmp = mkdtempSync(join(tmpdir(), 'wa-backfill-'));
  try {
    const download = deps.download ?? downloadMediaMessage;
    const normalized = { key: m.key, message: { documentMessage: found.doc } } as WAMessage;
    const buffer = (await download(
      normalized,
      'buffer',
      {},
      { reuploadRequest: sock.updateMediaMessage } as never,
    )) as Buffer;

    deps.logger.info({ bytes: buffer.length }, 'backfill: unzipping');
    new AdmZip(buffer).extractAllTo(tmp, /* overwrite */ true);

    const stats = await importFolder(tmp, {
      immich: deps.immich,
      dedup: deps.dedup,
      albumName,
      logger: deps.logger,
    });
    deps.logger.info({ ...stats, album: albumName }, 'backfill: import complete');

    if (jid) {
      const summary =
        `✅ Backfill done → album "${albumName}"\n` +
        `uploaded: ${stats.uploaded}, duplicate: ${stats.duplicate}, ` +
        `already-synced: ${stats.skippedDedup}, non-media: ${stats.skippedType}, errors: ${stats.errors}`;
      await sock.sendMessage(jid, { text: summary });
    }
  } catch (err) {
    deps.logger.error({ err: (err as Error).message }, 'backfill: ingest failed');
    if (jid) await sock.sendMessage(jid, { text: `❌ Backfill failed: ${(err as Error).message}` });
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
  return true;
}
