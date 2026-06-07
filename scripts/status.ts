import { getDedupDb } from '../src/config.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';

/** Print a quick summary of what has been synced so far. */
const dedup = new DedupStore(getDedupDb());

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

dedup.close();
