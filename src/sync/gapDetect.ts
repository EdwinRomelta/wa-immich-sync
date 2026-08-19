import type Database from 'better-sqlite3';

/**
 * Newest known WhatsApp send-time per group, across `synced` ∪ `outbox`.
 *
 * Both tables must be counted. A message sitting in the outbox during an
 * Immich outage has been *received* — reading only `synced` would call that
 * silence and report a gap that does not exist, which is precisely the
 * conflation Phase 1 removed from the startup path.
 *
 * Takes the raw connection rather than living on DedupStore or OutboxStore:
 * the query spans both tables and belongs to neither. Requires both stores to
 * have been constructed on this connection first (src/index.ts does that).
 *
 * `COALESCE(captured_at, created_at)` covers rows written before the
 * captured_at migration; their sync time is later than their send time, which
 * biases toward under-reporting a gap rather than inventing one.
 */
export function lastCapturedByGroup(db: Database.Database): Map<string, number> {
  const rows = db
    .prepare(
      `SELECT group_jid, MAX(at) AS at FROM (
         SELECT group_jid, COALESCE(captured_at, created_at) AS at FROM synced
         UNION ALL
         SELECT group_jid, captured_at AS at FROM outbox
       )
       GROUP BY group_jid`,
    )
    .all() as { group_jid: string; at: number }[];
  return new Map(rows.map((r) => [r.group_jid, r.at]));
}

/** A whitelisted group that used to deliver media and has since gone quiet. */
export interface GroupGap {
  groupJid: string;
  /** Newest send-time known for this group. */
  lastKnownAt: number;
  /** How long the group has been silent, in ms. */
  silentForMs: number;
}

/**
 * Whitelisted groups whose silence exceeds `thresholdMs`, longest first.
 *
 * A group with no entry in `lastKnown` is never reported. It has no baseline —
 * a newly whitelisted group, or one that has genuinely never posted media,
 * would otherwise look infinitely silent and alert on every reconnect forever.
 * A gap means "this used to work and stopped", which requires a before.
 */
export function detectGaps(opts: {
  lastKnown: Map<string, number>;
  groupJids: string[];
  now: number;
  thresholdMs: number;
}): GroupGap[] {
  const gaps: GroupGap[] = [];
  for (const groupJid of opts.groupJids) {
    const lastKnownAt = opts.lastKnown.get(groupJid);
    if (lastKnownAt === undefined) continue;
    const silentForMs = opts.now - lastKnownAt;
    if (silentForMs <= opts.thresholdMs) continue;
    gaps.push({ groupJid, lastKnownAt, silentForMs });
  }
  return gaps.sort((a, b) => b.silentForMs - a.silentForMs);
}

/** One human-readable line for an alert body. */
export function describeGap(gap: GroupGap): string {
  const hours = Math.floor(gap.silentForMs / 3_600_000);
  return `${gap.groupJid}: no media for ${hours}h (last seen ${new Date(gap.lastKnownAt).toISOString()})`;
}
