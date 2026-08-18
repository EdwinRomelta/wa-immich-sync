import type { AlertStore } from './alertStore.ts';

type AlertLogger = {
  info: (...a: unknown[]) => void;
  warn: (...a: unknown[]) => void;
  error: (...a: unknown[]) => void;
};

/** The slice of a Baileys WASocket this module needs; narrow so tests can fake it. */
export interface AlertSock {
  sendMessage: (jid: string, content: { text: string }) => Promise<unknown>;
  user?: { id?: string } | null;
}

export interface AlerterDeps {
  store: Pick<AlertStore, 'lastSentAt' | 'recordSent'>;
  /**
   * The live socket, or null. A getter rather than a value because the socket
   * is replaced on every reconnect (see startWaClient) — a captured reference
   * would go stale the first time the link drops.
   */
  getSock: () => AlertSock | null;
  /** ALERT_TARGET_JID; falls back to the bot's own number. */
  targetJid?: string;
  cooldownMs: number;
  logger: AlertLogger;
  /** Injectable clock (tests). */
  now?: () => number;
}

export type AlertOutcome = 'sent' | 'cooldown' | 'no-socket' | 'send-failed';

export interface Alerter {
  /**
   * Send `text` unless `condition` fired within the cooldown window.
   * Never throws — every caller is a timer tick or a socket event handler,
   * where an escaping rejection would kill the process under `restart: always`.
   */
  raise(condition: string, text: string): Promise<AlertOutcome>;
}

/**
 * Normalise a Baileys user id to a sendable JID.
 *
 * `sock.user.id` on a linked device carries a device suffix — "628123456:12
 * @s.whatsapp.net" — and sending to that literal string does not reach the
 * account. Strip everything from the ':' up to the '@'.
 */
export function selfJid(rawUserId: string | undefined): string | null {
  if (!rawUserId) return null;
  const at = rawUserId.indexOf('@');
  if (at <= 0) return null;
  const user = rawUserId.slice(0, at).split(':')[0];
  const domain = rawUserId.slice(at + 1);
  if (!user || !domain) return null;
  return `${user}@${domain}`;
}

export function createAlerter(deps: AlerterDeps): Alerter {
  const now = deps.now ?? Date.now;

  async function raise(condition: string, text: string): Promise<AlertOutcome> {
    const at = now();

    const last = deps.store.lastSentAt(condition);
    if (last !== null && at - last < deps.cooldownMs) return 'cooldown';

    const sock = deps.getSock();
    const jid = deps.targetJid ?? selfJid(sock?.user?.id);
    if (!sock || !jid) {
      // Not an error: WhatsApp being down is itself one of the conditions
      // worth alerting about, and the Docker healthcheck covers it. Crucially,
      // no cooldown is recorded, so the alert still lands once the link is back.
      deps.logger.warn({ condition }, 'alert not sent: no WhatsApp socket');
      return 'no-socket';
    }

    try {
      await sock.sendMessage(jid, { text });
    } catch (err) {
      // Also no cooldown recorded — a failed send must be retried on the next
      // tick, not silently swallowed for the next six hours.
      deps.logger.warn(
        { condition, err: err instanceof Error ? err.message : String(err) },
        'alert send failed',
      );
      return 'send-failed';
    }

    deps.store.recordSent(condition, at);
    deps.logger.info({ condition }, 'alert sent');
    return 'sent';
  }

  return { raise };
}
