import { getDedupDb } from '../src/config.ts';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';

/**
 * Print a quick summary of what has synced so far, and — as important —
 * what is still sitting in the outbox waiting on `drain`. The `synced`
 * count alone is blind to a stuck drain (bad API key, a poison row, Immich
 * down): it just freezes, with nothing to tell an operator whether 0 or
 * 4,000 photos are queued behind it. Both tables live on the same sqlite
 * connection, so open it once and share it, same as src/index.ts and
 * scripts/import-export.ts.
 */
const db = openDb(getDedupDb());
// DedupStore before OutboxStore: OutboxStore assumes the `synced` table
// (created by DedupStore's constructor) already exists on this connection.
const dedup = new DedupStore(db);
const outbox = new OutboxStore(db);

console.log('wa-immich-sync status');
console.log('---------------------');
console.log('Total synced assets:', dedup.count());

const last = dedup.lastSyncedAt();
console.log('Last synced at:     ', last ? new Date(last).toISOString() : 'never');

const byGroup = dedup.countByGroup();
if (byGroup.length > 0) {
  console.log('By group:');
  for (const row of byGroup) console.log(`  ${row.group_jid}: ${row.c}`);
}

console.log();
console.log('Outbox (captured, not yet in Immich)');
console.log('-------------------------------------');
const snapshot = outbox.snapshot();
console.log('Pending:             ', snapshot.depth);
console.log(
  'Oldest pending age:  ',
  snapshot.oldestPendingAgeMs === null ? 'n/a' : `${Math.round(snapshot.oldestPendingAgeMs / 1000)}s`,
);
console.log('Highest attempts:    ', snapshot.maxAttempts);
console.log('Most recent error:   ', snapshot.lastError ?? 'none');

dedup.close();
