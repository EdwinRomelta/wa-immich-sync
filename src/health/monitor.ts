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
        // void + .catch, not await: raise() calls sock.sendMessage, which can
        // hang forever on a half-open WhatsApp socket (the stall watchdog in
        // src/wa/client.ts exists precisely because that happens). Awaiting
        // it here would mean tick() never resolves, which means loop()'s
        // .finally() never re-arms the timer — the heartbeat stops for good,
        // not just for one cycle. raise() is contractually non-throwing; the
        // .catch is defense-in-depth, matching every other call site of it.
        void deps.alerter
          .raise(
            'outbox-depth',
            `wa-immich-sync: ${snapshot.depth} items queued and not yet in Immich ` +
              `(threshold ${deps.thresholds.outboxDepth}). Nothing is lost — they retry with backoff — ` +
              `but Immich has not been accepting uploads. Last error: ${snapshot.lastError ?? 'none'}`,
          )
          .catch((err) => {
            deps.logger.warn(
              { err: err instanceof Error ? err.message : String(err) },
              'health: outbox-depth alert threw',
            );
          });
      }

      const ageMs = snapshot.oldestPendingAgeMs;
      if (ageMs !== null && ageMs >= deps.thresholds.outboxAgeMs) {
        void deps.alerter
          .raise(
            'outbox-age',
            `wa-immich-sync: oldest queued item is ${Math.round(ageMs / 3_600_000)}h old ` +
              `(${snapshot.depth} queued, ${snapshot.maxAttempts} attempts). ` +
              `Last error: ${snapshot.lastError ?? 'none'}`,
          )
          .catch((err) => {
            deps.logger.warn(
              { err: err instanceof Error ? err.message : String(err) },
              'health: outbox-age alert threw',
            );
          });
      }
    } catch (err) {
      // better-sqlite3 is synchronous, so snapshot() throws in-band; caught
      // here so the timer loop below always re-arms. The alerter calls above
      // are void'd + .catch'd rather than awaited, so a hung raise() cannot
      // reach this catch (or block this tick) either — see the comment above.
      deps.logger.error(
        { err: err instanceof Error ? err.message : String(err) },
        'health: monitor tick failed',
      );
    }
  }

  function loop(): void {
    if (stopped) return;
    void tick()
      .catch((err) => {
        deps.logger.error(
          { err: err instanceof Error ? err.message : String(err) },
          'health: monitor tick rejected unexpectedly',
        );
      })
      .finally(() => {
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
