import type { GroupConfig } from '../types.ts';

/** Minimal group shape from Baileys' groupFetchAllParticipating. */
export interface GroupInfo {
  id: string;
  subject: string;
}

export interface ResolveResult {
  resolved: GroupConfig[];
  warnings: string[];
}

/**
 * Resolve raw whitelist entries (group names or jids) against the groups the
 * account actually participates in.
 *
 * - An entry containing "@" is treated as an exact jid.
 * - Otherwise it is matched against group names (exact, trimmed).
 * - A name matching multiple groups resolves to ALL of them (with a warning) so
 *   no data is silently dropped; a jid is the stable, unambiguous choice.
 * - Unmatched entries produce a warning and are skipped.
 *
 * The resolved list is de-duplicated by jid.
 */
export function resolveWhitelist(groups: GroupInfo[], entries: string[]): ResolveResult {
  const byId = new Map(groups.map((g) => [g.id, g]));
  const resolved = new Map<string, GroupConfig>();
  const warnings: string[] = [];

  for (const raw of entries) {
    const entry = raw.trim();
    if (!entry) continue;

    if (entry.includes('@')) {
      const g = byId.get(entry);
      if (g) resolved.set(g.id, { jid: g.id, name: g.subject });
      else warnings.push(`whitelist jid not among joined groups: ${entry}`);
      continue;
    }

    const matches = groups.filter((g) => g.subject.trim() === entry);
    if (matches.length === 0) {
      warnings.push(`whitelist name matched no joined group: "${entry}"`);
    } else if (matches.length > 1) {
      warnings.push(
        `whitelist name "${entry}" matches ${matches.length} groups — syncing all; ` +
          `use a jid to target one`,
      );
    }
    for (const g of matches) resolved.set(g.id, { jid: g.id, name: g.subject });
  }

  return { resolved: [...resolved.values()], warnings };
}
