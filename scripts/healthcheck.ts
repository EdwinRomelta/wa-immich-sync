import { getHealthFile, getHealthSettings } from '../src/config.ts';
import { readHeartbeat, type Heartbeat } from '../src/health/heartbeat.ts';

/**
 * Docker HEALTHCHECK entrypoint. Exit 0 = healthy, 1 = unhealthy.
 *
 * This is the backstop for the failure mode the design doc opens with: on
 * 2026-07-29 the daemon was simply not running for six days and nothing said
 * so. A WhatsApp alert cannot report a dead WhatsApp link, so this reads the
 * heartbeat file from outside the process instead.
 *
 * It checks liveness only. Immich being unreachable is deliberately NOT
 * unhealthy — media still queues durably to disk, which is the whole point of
 * the outbox. Outbox depth and age raise a WhatsApp alert instead.
 */
export function evaluateHealth(
  beat: Heartbeat | null,
  now: number,
  staleMs: number,
): { ok: boolean; reason: string } {
  if (beat === null) return { ok: false, reason: 'no heartbeat file (daemon never started, or it is unreadable)' };

  const daemonAge = now - beat.daemon;
  if (daemonAge > staleMs) {
    return { ok: false, reason: `daemon heartbeat stale by ${daemonAge - staleMs}ms (age ${daemonAge}ms)` };
  }

  // null means WhatsApp has not connected since this boot — normal during
  // pairing and during a long reconnect backoff. The daemon stamp already
  // proves the process is alive, and failing here would make a first boot
  // (or a QR-pairing wait) look like a fault.
  if (beat.wa !== null) {
    const waAge = now - beat.wa;
    if (waAge > staleMs) {
      return { ok: false, reason: `wa heartbeat stale by ${waAge - staleMs}ms (age ${waAge}ms)` };
    }
  }

  return { ok: true, reason: 'ok' };
}

// Only run when executed directly, so the unit test can import evaluateHealth
// without the import itself calling process.exit and killing the test runner.
if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  const { staleMs } = getHealthSettings();
  const beat = await readHeartbeat(getHealthFile());
  const result = evaluateHealth(beat, Date.now(), staleMs);
  console.log(result.ok ? `healthy: ${result.reason}` : `unhealthy: ${result.reason}`);
  process.exit(result.ok ? 0 : 1);
}
