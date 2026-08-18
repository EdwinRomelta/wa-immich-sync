# wa-immich-sync

Automatically copy photos and videos shared in **whitelisted WhatsApp groups** into a
self-hosted **[Immich](https://immich.app)** server — one album per group, no manual
download/upload. Plus a **backfill** path for older media via WhatsApp's "Export chat".

It links as an *additional* WhatsApp device (like WhatsApp Web) using
[Baileys](https://github.com/WhiskeySockets/Baileys), watches the groups you whitelist, and
pushes every new image/video to Immich. Uploads are idempotent, so it is safe to restart anytime.

> [!WARNING]
> **Unofficial client.** Baileys logs in as a linked device; this is not an official WhatsApp
> API and carries a small risk of the number being flagged. Use a number you control. The daemon
> only reads media — with one exception: it posts a short text **summary reply** in the dedicated
> backfill group after importing a zip.

## Features

- Live sync of images/videos from any number of whitelisted groups
- One Immich album per group (or a single album, or none)
- Whitelist groups by **name or JID**
- Backfill older media by dropping a WhatsApp chat-export `.zip` into a dedicated group
- Idempotent: local content-hash dedup **+** Immich's checksum dedup
- Runs as a small Node daemon or a Docker container

## How it works

```
WhatsApp (linked device)
   │  messages.upsert (live)  +  messaging-history.set (best-effort backfill)
   ▼
src/wa/client.ts ──▶ src/sync/ingest.ts ──▶ outbox (sqlite rows + staged files) ──▶ src/sync/drain.ts ──▶ Immich
                        │ 1. filter to whitelisted groups (resolved from names/JIDs)     │ 1. upload      (src/immich/client.ts)
                        │ 2. extract image/video       (src/wa/mediaExtractor.ts)        │ 2. add to album
                        │ 3. dedup check                (src/sync/dedupStore.ts, sqlite) └ 3. mark synced, remove row + file
                        └ 4. stage bytes to disk, insert an outbox row (src/sync/staging.ts)

Backfill group ──▶ src/sync/backfillIngest.ts ──▶ src/sync/importFolder.ts ──▶ outbox ──▶ src/sync/drain.ts ──▶ Immich
   (a .zip document is downloaded, unzipped, and every photo/video staged + queued
    the same way as live media — there is no separate upload path)
```

Media is durable on disk — fsynced, then the outbox row committed to sqlite — before any upload is
even attempted. `ingest` never talks to Immich directly, so an Immich outage never blocks WhatsApp
or drops a message: it just staged, waiting for `drain` to catch up. `drain` runs on a timer (plus
an immediate first pass on boot so a backlog doesn't sit idle), retries failed uploads with
exponential backoff, and only removes a row (and its staged file) once Immich confirms the upload.
A startup sweep clears any staged file left behind by a crash between staging and the row insert.

### Knowing when it stops

Durability answers "can a photo be lost while the daemon runs". It says nothing
about the daemon not running — an outage on 2026-07-29 went unnoticed for six
days. Three signals cover that:

- **Docker healthcheck.** The daemon rewrites `HEALTH_FILE` every
  `HEALTH_INTERVAL_MS`; the container is marked `unhealthy` once that stamp is
  older than `HEALTH_STALE_MS`. Check it with `docker ps` or run it by hand with
  `npm run healthcheck`. It reports liveness only — **an unreachable Immich is
  deliberately not unhealthy**, because media still queues durably to disk.
- **WhatsApp alerts.** The bot messages `ALERT_TARGET_JID` (its own number by
  default) when the outbox passes `ALERT_OUTBOX_DEPTH` or `ALERT_OUTBOX_AGE_MS`,
  when a whitelisted group has been silent longer than
  `CATCHUP_GAP_THRESHOLD_MS`, when WhatsApp has failed to reconnect
  `ALERT_RECONNECT_FAILURES` times in a row, or when a message could not be
  captured to disk at all. Each condition is rate-limited to one message per
  `ALERT_COOLDOWN_MS`, and that cooldown is persisted, so a restart loop cannot
  turn into a notification storm.
- **`npm run status`.** Adds a Health section, last-media-per-group send times,
  and which alerts have fired.

A detected gap is **reported, not recovered** — WhatsApp does not reliably
re-deliver a window a linked device missed. Fill it by re-importing a chat
export into the backfill group.

## Prerequisites

- Node.js **≥ 22** (uses built-in `fetch`/`FormData` and `process.loadEnvFile`)
- A running Immich server and an **API key** (Immich → Account Settings → API Keys).
  The key needs `asset.upload`, `album.read`, `album.create`, `albumAsset.create` (or full access).
- A WhatsApp account with a free linked-device slot (WhatsApp allows ~4)

## Quick start

```bash
git clone https://github.com/<your-username>/wa-immich-sync.git
cd wa-immich-sync
npm install

cp .env.example .env
#   edit .env: IMMICH_URL, IMMICH_API_KEY, WHITELIST_GROUPS
```

### 1. Pair the device (one time)

```bash
npm run pair
```

Scan the QR: **WhatsApp → Linked Devices → Link a device**. Auth is saved under `data/auth/`.
Press `Ctrl+C` once connected.

### 2. Find your group names

```bash
npm run list-groups
```

Copy the group names (or JIDs) you want into `WHITELIST_GROUPS` in `.env`.

### 3. Run

```bash
npm run dev            # foreground (development)
# or
docker compose up -d   # background daemon (restart: unless-stopped)
```

Post a photo in a whitelisted group and watch it appear in Immich (album + main timeline).

## Configuration

All configuration is via environment variables (see `.env.example`):

| Var | Required | Default | Meaning |
|---|---|---|---|
| `IMMICH_URL` | ✓ | — | Immich base URL |
| `IMMICH_API_KEY` | ✓ | — | Immich API key |
| `WHITELIST_GROUPS` | ✓ | — | Comma-separated group **names or JIDs** to sync |
| `BACKFILL_GROUP_NAME` | | `wa-immich-backfill` | Group whose `.zip` uploads get imported |
| `ALBUM_MODE` | | `per-group` | `per-group` \| `single` \| `none` |
| `SINGLE_ALBUM_NAME` | | `WhatsApp` | Album name when `ALBUM_MODE=single` |
| `MEDIA_TYPES` | | `image,video` | Subset of `image,video` |
| `BACKFILL` | | `true` | Request WhatsApp history sync on link |
| `WA_AUTH_DIR` | | `./data/auth` | Where Baileys stores auth |
| `DEDUP_DB` | | `./data/synced.db` | sqlite dedup database |
| `OUTBOX_DIR` | | `./data/outbox` | Where media is staged before upload. Must be a directory used for nothing else: the daemon refuses to start if it is the same as, contains, or sits inside `WA_AUTH_DIR` or the `DEDUP_DB` **file** itself (not just its parent directory — `OUTBOX_DIR=./data/outbox` alongside `DEDUP_DB=./data/synced.db` is the shipped default and stays legal), and it also refuses to start if the directory already has unrelated files in it. Its startup sweep deletes every unrecognised file from this directory, so give it a dedicated, empty directory. |
| `DRAIN_INTERVAL_MS` | | `30000` | How often the drain loop checks for due rows (plus one immediate pass on boot) |
| `DRAIN_BATCH_SIZE` | | `10` | Rows processed per drain tick |
| `DRAIN_BASE_BACKOFF_MS` | | `30000` | Starting delay before a failed upload is retried |
| `DRAIN_MAX_BACKOFF_MS` | | `3600000` | Backoff ceiling; raised to `DRAIN_BASE_BACKOFF_MS` if set lower |
| `DRAIN_DROP_AFTER_ATTEMPTS` | | `3` | Retries a row earns before an unusable staged file is treated as terminal and dropped |
| `DRAIN_MAX_DROPS_PER_TICK` | | `5` | Cap on terminal drops per tick, so a directory outage can't empty the queue |
| `HEALTH_FILE` | | `./data/health.json` | Liveness file the Docker healthcheck reads. Subject to the same overlap guard as `DEDUP_DB` and `WA_AUTH_DIR`: it must not sit inside `OUTBOX_DIR` |
| `HEALTH_INTERVAL_MS` | | `60000` | How often the daemon stamps the heartbeat and checks the outbox backlog |
| `HEALTH_STALE_MS` | | `3600000` | Heartbeat age at which the container is reported `unhealthy` |
| `ALERT_TARGET_JID` | | own number | Where WhatsApp alerts are sent |
| `ALERT_COOLDOWN_MS` | | `21600000` | Minimum gap between repeats of one alert condition |
| `ALERT_OUTBOX_DEPTH` | | `50` | Queued items before alerting that Immich is not accepting uploads |
| `ALERT_OUTBOX_AGE_MS` | | `7200000` | Age of the oldest queued item before alerting |
| `ALERT_RECONNECT_FAILURES` | | `10` | Consecutive WhatsApp reconnect failures before alerting |
| `CATCHUP_GAP_THRESHOLD_MS` | | `3600000` | Per-group silence before a gap is reported |

**Whitelist by name or JID.** Each `WHITELIST_GROUPS` entry is matched by group name, or treated
as an exact JID if it contains `@g.us`. If a name matches **multiple** groups, all of them are
synced and a warning is logged — use a JID to target exactly one. A JID is also stable if the
group is later renamed.

## Backfill (older media)

WhatsApp only delivers a limited, recent slice of history to a newly linked device, and it
**never** gives a member messages sent *before that member joined*. So media older than the bot's
membership cannot be pulled through the live connection at all. Two reliable options:

### Option A — drop a chat export into the backfill group (recommended)

1. Create a group named `wa-immich-backfill` (or your `BACKFILL_GROUP_NAME`) and add the bot.
2. On a phone that **has** the photos: WhatsApp → the source chat → **Export chat → Attach Media**.
3. Send the resulting `.zip` into the backfill group **as a Document** (not as photos — documents
   are not recompressed, so original files and dates are preserved). Optionally set the message
   **caption** to the target album name (otherwise a default album is used).
4. The bot downloads, unzips, imports every photo/video, and replies with a summary.

### Option B — import a folder directly (CLI)

```bash
npm run import -- /path/to/exported/folder --album "My Album"
```

Both paths parse dates from WhatsApp filenames (`IMG-YYYYMMDD-WA####`), falling back to file mtime.

> WhatsApp's "Export with media" caps the number of exported files. For very large chats, copy the
> phone's `WhatsApp/Media/WhatsApp Images` (and `WhatsApp Video`) folders and import those instead.

## Deduplication

Two layers make every path idempotent:

- **Local (sqlite):** import dedup is keyed by the **SHA-1 of the file content**, so the same photo
  re-exported by a different person (different filename) is skipped. Live sync dedups by WhatsApp
  message id.
- **Immich:** the server dedups by checksum, so a duplicate upload never creates a second asset.

A re-compressed or edited copy is genuinely different bytes and is treated as a new asset.

## Docker

```bash
docker compose up -d      # start
docker compose logs -f    # watch
docker compose down       # stop
```

`./data` is mounted so WhatsApp auth + the dedup db survive restarts. To reach an Immich server on
the host, set `IMMICH_URL=http://host.docker.internal:2283`. If Immich runs in its own Docker
stack, see the commented `networks:` block in `docker-compose.yml`.

## Scripts

| Command | Purpose |
|---|---|
| `npm run pair` | One-time device pairing (QR) |
| `npm run list-groups` | Print the groups you are in (names + JIDs) |
| `npm run dev` / `npm start` | Run the sync daemon |
| `npm run import -- <folder>` | Import an exported-chat folder |
| `npm run status` | Synced counts, outbox depth/age/errors, daemon health, last media per group, and which alerts have fired |
| `npm run healthcheck` | Run the Docker healthcheck by hand; exits non-zero when the heartbeat is stale |
| `npm test` / `npm run typecheck` | Tests (all I/O mocked) / type check |

## Caveats

- Best-effort live backfill: WhatsApp only syncs recent, post-join history to a linked device.
- The daemon sends one summary message per imported zip in the backfill group.
- `.env` and `data/` (WhatsApp credentials) are gitignored — never commit them.

## License

[MIT](./LICENSE)
