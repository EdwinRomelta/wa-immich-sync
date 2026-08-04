export interface StallWatchdogOpts {
  /** Fire onStall when no touch() for this long. */
  timeoutMs: number;
  /** How often to check. */
  checkEveryMs: number;
  /** Called once when the stall is detected; the watchdog stops itself. */
  onStall: (idleMs: number) => void;
  /** Injectable clock (tests). */
  now?: () => number;
}

export interface StallWatchdog {
  /** Record activity — resets the idle clock. */
  touch(): void;
  stop(): void;
}

/**
 * Detects a silently dead WhatsApp socket: a healthy Baileys link exchanges
 * keepalive frames every ~30s, so prolonged total silence means the connection
 * is a zombie (server pushes messages into a dead socket and they are lost
 * forever). Fires once, then stops — caller is expected to tear down and
 * reconnect.
 */
export function startStallWatchdog(opts: StallWatchdogOpts): StallWatchdog {
  const now = opts.now ?? Date.now;
  let lastActivity = now();

  const timer = setInterval(() => {
    const idleMs = now() - lastActivity;
    if (idleMs > opts.timeoutMs) {
      clearInterval(timer);
      opts.onStall(idleMs);
    }
  }, opts.checkEveryMs);
  // Never keep the process alive just for the watchdog.
  timer.unref?.();

  return {
    touch(): void {
      lastActivity = now();
    },
    stop(): void {
      clearInterval(timer);
    },
  };
}
