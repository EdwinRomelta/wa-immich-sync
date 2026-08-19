import { getDedupDb, getHealthFile, getHealthSettings } from '../src/config.ts';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import { AlertStore } from '../src/alert/alertStore.ts';
import { readHeartbeat } from '../src/health/heartbeat.ts';
import { lastCapturedByGroup } from '../src/sync/gapDetect.ts';

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
const alerts = new AlertStore(db);

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

console.log();
console.log('Health');
console.log('------');
const beat = await readHeartbeat(getHealthFile());
const { staleMs } = getHealthSettings();
if (beat === null) {
  console.log('Heartbeat:            none (daemon not running, or HEALTH_FILE is elsewhere)');
} else {
  const daemonAge = Date.now() - beat.daemon;
  console.log(`Daemon heartbeat:     ${Math.round(daemonAge / 1000)}s ago${daemonAge > staleMs ? '  <-- STALE' : ''}`);
  if (beat.wa === null) {
    console.log('WhatsApp activity:    never this boot');
  } else {
    const waAge = Date.now() - beat.wa;
    console.log(`WhatsApp activity:    ${Math.round(waAge / 1000)}s ago${waAge > staleMs ? '  <-- STALE' : ''}`);
  }
}

console.log();
console.log('Last media per group (send time)');
console.log('---------------------------------');
const lastSeen = [...lastCapturedByGroup(db).entries()].sort((a, b) => a[1] - b[1]);
if (lastSeen.length === 0) console.log('  (nothing captured yet)');
for (const [jid, at] of lastSeen) {
  console.log(`  ${jid}: ${new Date(at).toISOString()} (${Math.round((Date.now() - at) / 3_600_000)}h ago)`);
}

console.log();
console.log('Alerts sent');
console.log('-----------');
const sent = alerts.all();
if (sent.length === 0) console.log('  (none)');
for (const a of sent) console.log(`  ${a.condition}: ${new Date(a.lastSentAt).toISOString()}`);

dedup.close();
