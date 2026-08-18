FROM node:22-bookworm-slim

WORKDIR /app

# Build toolchain for better-sqlite3 native bindings (used if no prebuilt binary).
RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 make g++ \
  && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm install --omit=dev

COPY . .

ENV NODE_ENV=production

# Liveness only. Reads the heartbeat file src/health/monitor.ts rewrites; it
# never contacts Immich, because an Immich outage is healthy queueing and must
# not mark this container unhealthy. start-period covers first-boot QR pairing.
HEALTHCHECK --interval=60s --timeout=20s --start-period=180s --retries=3 \
  CMD ["./node_modules/.bin/tsx", "scripts/healthcheck.ts"]

# Exec the tsx binary directly rather than going through `npx`. As PID 1, npm
# ran the app as a `sh -c` grandchild, so SIGTERM never reached the process:
# the SIGINT/SIGTERM handler in src/index.ts never ran, the sqlite db was never
# closed (leaving an unbounded WAL), and every clean stop reported exit code 1
# as if it had crashed.
CMD ["./node_modules/.bin/tsx", "src/index.ts"]
