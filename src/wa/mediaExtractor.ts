import { downloadMediaMessage, type WAMessage, type WASocket } from '@whiskeysockets/baileys';
import type { AppConfig, MediaItem, MediaKind } from '../types.ts';

/** Minimal shape of the socket bits the extractor needs (eases testing). */
type DownloadSock = Pick<WASocket, 'updateMediaMessage'>;

export interface ExtractDeps {
  /** Injectable downloader for tests. */
  download?: typeof downloadMediaMessage;
  logger?: { error: (...args: unknown[]) => void };
}

type AnyMessage = Record<string, any> | null | undefined;

/** Unwrap common WhatsApp message wrappers to reach the real content. */
function unwrap(message: AnyMessage): AnyMessage {
  if (!message) return message;
  return (
    message.ephemeralMessage?.message ??
    message.viewOnceMessage?.message ??
    message.viewOnceMessageV2?.message ??
    message.viewOnceMessageV2Extension?.message ??
    message.documentWithCaptionMessage?.message ??
    message
  );
}

interface Detected {
  kind: MediaKind;
  mime: string;
  ext: string;
}

function detectKind(content: AnyMessage): Detected | null {
  if (content?.imageMessage) {
    return { kind: 'image', mime: content.imageMessage.mimetype || 'image/jpeg', ext: 'jpg' };
  }
  if (content?.videoMessage) {
    return { kind: 'video', mime: content.videoMessage.mimetype || 'video/mp4', ext: 'mp4' };
  }
  return null;
}

/**
 * Inspect a WhatsApp message; if it carries image/video media allowed by
 * config, download it and return a MediaItem. Otherwise return null.
 */
export async function extractMedia(
  sock: DownloadSock,
  m: WAMessage,
  config: AppConfig,
  groupName: string,
  deps: ExtractDeps = {},
): Promise<MediaItem | null> {
  const content = unwrap(m.message as AnyMessage);
  const detected = detectKind(content);
  if (!detected) return null;
  if (!config.mediaTypes.includes(detected.kind)) return null;

  const groupJid = m.key?.remoteJid ?? '';
  const rawId = m.key?.id ?? '';
  if (!groupJid || !rawId) return null;

  // Download from a normalized message so wrapped (ephemeral/view-once) media works.
  const normalized = { key: m.key, message: content } as WAMessage;
  const download = deps.download ?? downloadMediaMessage;
  const buffer = (await download(
    normalized,
    'buffer',
    {},
    { reuploadRequest: sock.updateMediaMessage } as never,
  )) as Buffer;

  const tsRaw = m.messageTimestamp;
  const tsSec = typeof tsRaw === 'number' ? tsRaw : Number(tsRaw ?? 0);
  const timestamp = tsSec > 0 ? new Date(tsSec * 1000) : new Date();

  return {
    messageId: `${groupJid}:${rawId}`,
    rawMessageId: rawId,
    groupJid,
    groupName,
    kind: detected.kind,
    mimeType: detected.mime,
    fileName: `${rawId}.${detected.ext}`,
    timestamp,
    buffer,
  };
}
