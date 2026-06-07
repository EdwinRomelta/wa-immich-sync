import pino from 'pino';

// Load .env (if present) before reading LOG_LEVEL. Node >= 20.6 built-in.
try {
  process.loadEnvFile();
} catch {
  // no .env file — fall back to the real environment
}

export const logger = pino({ level: process.env.LOG_LEVEL ?? 'info' });

export type Logger = typeof logger;
