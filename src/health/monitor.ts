import type { Alerter } from '../alert/alerter.ts';
import type { OutboxStore } from '../sync/outboxStore.ts';
import { writeHeartbeat } from './heartbeat.ts';

type MonitorLogger = {
  info: (...a: unknown[]) => void;
  warn: (...a: unknown[]) => void;
  error: (...a: unknown[]) => void;
};

export interface HealthMonitorDeps {
  outbox: Pick<OutboxStore, 'snapshot'>;
  alerter: Pick<Alerter, 'raise'>;
  heartbeatPath: string;
  /** Epoch ms of the last WhatsApp activity, or null before the first connect. */
  waActivity: () => number | null;
  thresholds: { outboxDepth: number; outboxAgeMs: number };
  intervalMs: number;
  logger: MonitorLogger;
  /** Injectable clock (tests). */
  now?: () => number;
  /** Start the timer loop. Tests drive tick() by hand instead. */
  autoStart?: boolean;
}

export interface HealthMonitor {
  stop(): void;
  tick(): Promise<void>;
}

/**
 * Periodic liveness and backlog check.
 *
 * The heartbeat is written FIRST and unconditionally, before anything that can
 * throw. A wedged sqlite read or a failing alert must not also make the
 * container look dead — those are separate faults with separate signals.
 *
 * Nothing here touches Immich or the WhatsApp connection state. Depth and age
 * are read from columns the outbox already maintains, so a stuck drain is
 * inferred from its backlog rather than probed for.
 */
export function startHealthMonitor(deps: HealthMonitorDeps): HealthMonitor {
  const now = deps.now ?? Date.now;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let stopped = false;

  async function tick(): Promise<void> {
    const at = now();

    try {
      await writeHeartbeat(deps.heartbeatPath, { daemon: at, wa: deps.waActivity() });
    } catch (err) {
      deps.logger.warn(
        { err: err instanceof Error ? err.message : String(err), path: deps.heartbeatPath },
        'health: heartbeat write failed',
      );
    }

    try {
      const snapshot = deps.outbox.snapshot(at);

      if (snapshot.depth >= deps.thresholds.outboxDepth) {
        await deps.alerter.raise(
          'outbox-depth',
          `wa-immich-sync: ${snapshot.depth} items queued and not yet in Immich ` +
            `(threshold ${deps.thresholds.outboxDepth}). Nothing is lost — they retry with backoff — ` +
            `but Immich has not been accepting uploads. Last error: ${snapshot.lastError ?? 'none'}`,
        );
      }

      const ageMs = snapshot.oldestPendingAgeMs;
      if (ageMs !== null && ageMs >= deps.thresholds.outboxAgeMs) {
        await deps.alerter.raise(
          'outbox-age',
          `wa-immich-sync: oldest queued item is ${Math.round(ageMs / 3_600_000)}h old ` +
            `(${snapshot.depth} queued, ${snapshot.maxAttempts} attempts). ` +
            `Last error: ${snapshot.lastError ?? 'none'}`,
        );
      }
    } catch (err) {
      // better-sqlite3 is synchronous, so snapshot() throws in-band; the
      // alerter is async but documented never to throw. Contain both here so
      // the timer loop below always re-arms.
      deps.logger.error(
        { err: err instanceof Error ? err.message : String(err) },
        'health: monitor tick failed',
      );
    }
  }

  function loop(): void {
    if (stopped) return;
    void tick().finally(() => {
      if (!stopped) timer = setTimeout(loop, deps.intervalMs);
    });
  }

  function stop(): void {
    stopped = true;
    if (timer) clearTimeout(timer);
    timer = null;
  }

  if (deps.autoStart !== false) timer = setTimeout(loop, deps.intervalMs);

  return { stop, tick };
}
