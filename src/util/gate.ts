/**
 * One-shot async gate: callers block on wait() until open() is called.
 * Used to hold message processing until the group whitelist is resolved —
 * WhatsApp can deliver the initial history batch milliseconds after connect,
 * before groupFetchAllParticipating() returns (whitelist race).
 */
export interface Gate {
  open(): void;
  wait(): Promise<void>;
  readonly isOpen: boolean;
}

export function createGate(): Gate {
  let opened = false;
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });

  return {
    open(): void {
      opened = true;
      release();
    },
    wait(): Promise<void> {
      return promise;
    },
    get isOpen(): boolean {
      return opened;
    },
  };
}
