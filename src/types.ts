/** Shared domain types for wa-immich-sync. */

export type MediaKind = 'image' | 'video';

export type AlbumMode = 'per-group' | 'single' | 'none';

/** A resolved WhatsApp group (jid known) the daemon syncs. */
export interface GroupConfig {
  /** Group JID, e.g. "<digits>-<digits>@g.us" or "<digits>@g.us". */
  jid: string;
  /** Human name; also used as the Immich album name in per-group mode. */
  name: string;
}

/** Validated sync settings, loaded from environment variables. */
export interface AppConfig {
  /** Raw whitelist entries — each is a group name or a jid (resolved at runtime). */
  whitelist: string[];
  mediaTypes: MediaKind[];
  backfill: boolean;
  albumMode: AlbumMode;
  /** Album name used when albumMode === 'single'. Defaults to "WhatsApp". */
  singleAlbumName?: string;
  /** Name of a dedicated group where exported-chat .zip archives are imported. */
  backfillGroupName: string;
  /** Emoji to react with on each synced message; unset/empty = feature off. */
  reactionEmoji?: string;
}

/** A downloadable piece of media extracted from a WhatsApp message. */
export interface MediaItem {
  /** Stable dedup key: `${groupJid}:${rawMessageId}`. */
  messageId: string;
  /** Raw WhatsApp message id (m.key.id). */
  rawMessageId: string;
  groupJid: string;
  groupName: string;
  kind: MediaKind;
  mimeType: string;
  fileName: string;
  timestamp: Date;
  buffer: Buffer;
}

/** Result of an Immich upload. */
export interface UploadResult {
  assetId: string;
  status: 'created' | 'replaced' | 'duplicate';
}
