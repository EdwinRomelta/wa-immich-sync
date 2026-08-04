# Photo Gap Prevention — Design

Date: 2026-08-04
Status: approved, not yet implemented

## Problem

Photos posted to a whitelisted WhatsApp group can go missing permanently. It has
happened twice, by two different mechanisms.

**Gap A — never received.** The daemon is down. WhatsApp's offline replay does
not reach back far enough, and sender keys rotate while the device is offline, so
the bytes never arrive in the process at all. This caused the outage from
2026-07-29 05:07 WIB to 2026-08-04 09:17 WIB: six days, 0 assets synced, no
notification to anyone.

**Gap B — received but lost.** A message arrives, Immich is unreachable, the
upload throws, and the message is dropped from the in-flight path with no record
that it ever existed. `src/index.ts` documents an instance: "2026-07-08 photos
were lost this way".

Storing media in the database before uploading to Immich addresses only B. A
staging table cannot stage bytes that were never fetched. Both mechanisms need
fixing, because either one alone still leaves a live hole.

### Why A cannot currently self-heal

`startBackfill` pages *backward* through history via `fetchMessageHistory`, and
`OldestAnchors.note()` keeps the **oldest** timestamp per chat
(`src/sync/backfill.ts:20`). The cursor is seeded from `dedup.newestByGroup()` —
the newest *already-synced* message — so it walks backward into history that is
already present. Nothing traverses the window between last-synced and now.

Backfill is an initial-history tool, not a downtime catch-up tool. On 2026-08-04
it logged "complete — all chats exhausted" while the six-day gap sat untouched.
The catch-up traversal does not exist and must be built.

## Goals

- A received photo cannot be lost by an Immich outage or a process crash.
- A downtime window is detected, partially recovered automatically, and the
  unrecoverable remainder is reported precisely.
- Downtime becomes visible in hours, not days.

## Non-goals

- Recovering media WhatsApp has already expired or that can no longer be
  decrypted. This is a hard server-side limit; the design reports it instead of
  pretending to solve it.
- Replacing Immich as the source of truth. Local staging is transient.
- Migrating to Immich external libraries. Rejected: read-only assets, loss of
  album-per-group assignment, different checksum dedup, and a container mount
  requirement — too much collateral change.

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Scope | Fix both A and B | Each alone leaves a live hole |
| Alerting | WhatsApp message to self | Bot already holds an authenticated socket; zero new infra |
| Staged copy retention | Delete after Immich confirms | Transient outbox; Immich stays source of truth; no unbounded disk |
| Downtime recovery | Bounded auto catch-up, zip fallback for the rest | Automatic where safe, honest where not |
| Architecture | Outbox-centric split | Makes the happy path and the recovery path the same path |

### Why outbox-centric over a minimal bolt-on

A bolt-on wraps the upload in a `catch` that spills to disk and retries. It is a
smaller diff, but it recreates the exact structural weakness that caused Gap B: a
rarely-taken failure branch that only executes when things are already broken,
and is therefore never exercised.

With an outbox there is no failure branch. Immich being unavailable simply means
rows stay queued. Retry is the absence of progress, not special-case code.

## Architecture

```
WhatsApp ──> ingest ──> [outbox table + staged file] ──> drain ──> Immich
                │                                          │
             dedup                                    on success:
        (synced ∪ outbox)                     insert synced, delete outbox,
                                                     unlink file
```

`ingest` and `drain` are fully decoupled. `drain` never calls WhatsApp.

### Data model

`synced` keeps its role as the permanent record. A new `outbox` table holds work
in flight. A row exists in exactly one of the two.

```sql
CREATE TABLE outbox (
  message_id  TEXT PRIMARY KEY,   -- same "${jid}:${rawId}" key as synced
  group_jid   TEXT NOT NULL,
  album_name  TEXT NOT NULL,      -- resolved at ingest; drain needs no WA access
  file_path   TEXT NOT NULL,
  file_name   TEXT NOT NULL,
  mime_type   TEXT NOT NULL,
  captured_at INTEGER NOT NULL,   -- WA send time -> Immich fileCreatedAt
  attempts    INTEGER NOT NULL DEFAULT 0,
  last_error  TEXT,
  created_at  INTEGER NOT NULL,
  next_try_at INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_outbox_ready ON outbox(next_try_at, created_at);
```

One additive migration to `synced`:

```sql
ALTER TABLE synced ADD COLUMN captured_at INTEGER;  -- nullable
```

`synced.created_at` records when a row was *synced*, not when the message was
*sent*. For live syncs these are close; for zip-imported or backfilled rows they
diverge sharply. Gap detection needs send time. Existing rows become NULL and
fall back to `created_at`. The migration is non-destructive and guarded so it is
idempotent.

All timestamps are integer epoch milliseconds, matching the existing
`synced.created_at`.

Everything `drain` needs is captured at ingest, so a retry hours later is
byte-identical to the first attempt.

Dedup becomes `synced.has(id) || outbox.has(id)`: a pending message is
already-have, so redelivery will not re-download it.

`next_try_at` uses the existing `backoffDelayMs` helper (`src/util/backoff.ts`)
for per-item exponential backoff, so an unreachable Immich does not spin the
queue.

### Crash-safety ordering

Write to `outbox/tmp/<id>`, fsync, rename into `outbox/<id>`, **then** insert the
row. Rename is atomic, so a crash can only leave an orphan file with no row —
never a row pointing at a missing or truncated file. The reverse order would be
unsafe. A startup sweep deletes orphans.

On success: insert `synced` and delete `outbox` in one transaction, then unlink.
A crash before unlink leaves an orphan (swept later); a crash before commit
retries the item. Both are harmless.

### Ingest — `src/sync/ingest.ts`

1. Resolve group against whitelist; skip if not listed
2. Dedup check across `synced` ∪ `outbox`; skip if known
3. Extract media via the existing `mediaExtractor`, keeping its `withRetry`
   download
4. Atomic write into the outbox directory
5. Insert the outbox row with the resolved album name

Returns `queued | duplicate | skipped-no-media | not-whitelisted`, matching the
outcome vocabulary the current pipeline reports, so logging and tests carry over.

### Drain — `src/sync/drain.ts`

```
every intervalMs:
  rows = store.dueOutbox(now, batchSize)      // next_try_at <= now
  for row:
    upload -> ensureAlbum -> addToAlbum
    success -> complete(row, assetId)         // tx: insert synced, delete outbox, unlink
    failure -> defer(row, err, backoffDelayMs(attempts, ...))
```

Drain reads staged files with `fs.openAsBlob()` (Node 22) so large videos stream
rather than loading whole into memory. `ImmichClient.uploadAsset` is refactored to
build a Blob and delegate to a shared `uploadBlob`, giving one upload
implementation and two entry points; existing buffer-based tests keep passing.

The zip importer (`backfillIngest` / `importFolder`) routes through the same
outbox. Leaving it on a direct upload would rebuild the very thing this design
removes: a second upload path with its own failure behaviour.

### Consequence: delete the Immich startup gate

`src/index.ts:41` currently blocks the WhatsApp connect on
`withRetry(() => immich.ping(), { retries: 120 })`. After roughly 20 minutes it
throws, reaches `main().catch`, and exits the process — a crash mode that exists
only because an in-flight message had nowhere safe to go.

With an outbox, ingest queues while Immich is down, so WhatsApp connects
immediately and this gate is deleted. The outbox removes one of the two ways this
service has historically died.

### Catch-up — `src/sync/catchup.ts`

1. On reconnect, after whitelist resolution, compute per group
   `lastKnown = max(captured_at ?? created_at)` across `synced` ∪ `outbox`
2. If `now - lastKnown > threshold` (1 hour), suspect a gap
3. Obtain the **newest** message key for the group, from the first live/`append`
   message or from `messaging-history.set`, whichever arrives first
4. Page backward using a **separate cursor** from `OldestAnchors`. Reusing that
   cursor is the bug described above and must not be repeated
5. Stop on reaching `lastKnown`, hitting the page cap, or stalling
6. Route every media message found through the same `ingest`; dedup makes
   overlap free
7. Pace requests at the existing 10s pump interval to protect the shared
   WhatsApp account

`ChatBackfill`'s stall detection is the correct shape and is generalised rather
than duplicated.

Messages that cannot be decrypted are counted and reported, not silently dropped.
That residual is what the zip fallback exists for.

### Alerting — `src/alert/`

A WhatsApp message to a configurable target JID.

Triggers:
- Gap detected on reconnect: range, count recovered, range still missing
- Outbox depth or oldest-pending age over threshold (Immich stuck)
- N consecutive reconnect failures

Alerts are rate-limited to one per condition per cooldown, persisted across
restarts, so a bad night cannot produce hundreds of messages.

### Heartbeat backstop

If WhatsApp is the broken component, no WhatsApp alert can send. Each successful
drain cycle and each connection-open stamps `last_ok`. A Docker `HEALTHCHECK`
reads it and fails when stale, marking the container `unhealthy` in `docker ps`.

This does not notify, but it makes a hung process *visible* rather than silent —
the precise failure that went unnoticed for six days.

## Configuration defaults

Every threshold has a concrete default so behaviour is not left to the
implementer's judgement. All are overridable by environment variable.

| Setting | Default | Meaning |
|---|---|---|
| `OUTBOX_DIR` | `./data/outbox` | Staging directory |
| `DRAIN_INTERVAL_MS` | 30000 | Drain loop tick |
| `DRAIN_BATCH_SIZE` | 10 | Rows attempted per tick |
| `DRAIN_BASE_BACKOFF_MS` | 30000 | Per-item retry base |
| `DRAIN_MAX_BACKOFF_MS` | 3600000 | Per-item retry cap (1 hour) |
| `CATCHUP_GAP_THRESHOLD_MS` | 3600000 | Silence before a gap is suspected (1 hour) |
| `CATCHUP_PAGE_SIZE` | 50 | Messages per history page |
| `CATCHUP_INTERVAL_MS` | 10000 | Pacing between pages; protects the shared account |
| `CATCHUP_MAX_PAGES` | 200 | Hard cap per group per reconnect |
| `CATCHUP_MAX_STALLS` | 3 | No-progress steps before giving up |
| `ALERT_TARGET_JID` | own number | Where alerts are sent |
| `ALERT_COOLDOWN_MS` | 21600000 | Minimum gap between repeats of one condition (6 hours) |
| `ALERT_OUTBOX_DEPTH` | 50 | Pending rows before alerting |
| `ALERT_OUTBOX_AGE_MS` | 7200000 | Oldest pending age before alerting (2 hours) |
| `ALERT_RECONNECT_FAILURES` | 10 | Consecutive failures before alerting |
| `HEALTH_STALE_MS` | 3600000 | `last_ok` age at which the healthcheck fails (1 hour) |

## Implementation phasing

This design is one coherent system but too large for a single implementation
plan. It splits into three independently shippable phases, each leaving the
service in a working state. Each phase gets its own plan.

**Phase 1 — Outbox (fixes Gap B).** Outbox table and store, atomic staging
writes, orphan sweep, `ingest`, `drain`, `uploadBlob` refactor, zip importer
rerouted, Immich startup gate deleted. Largest phase and the highest value: after
this, no received photo can be lost to an Immich outage or a crash.

**Phase 2 — Detection and alerting (fixes Gap A visibility).** `captured_at`
migration, gap detection on reconnect, WhatsApp alerting with cooldown,
`last_ok` heartbeat, Docker `HEALTHCHECK`. Depends on Phase 1 for outbox depth
and age signals. This is the phase that would have turned six days into a few
hours.

**Phase 3 — Catch-up (recovers Gap A).** Catch-up cursor, backward traversal,
generalised stall detection, undecryptable-message reporting. Depends on Phase 1
for `ingest` and Phase 2 for gap detection and the report channel.

Recommended order is 1 → 2 → 3. Phase 2 delivers more risk reduction per unit of
work than Phase 3, because being told early beats recovering late.

## Testing

Unit tests, with injected clock, filesystem, and randomness to match the existing
style:

- outbox store: insert, due selection, complete, defer, dedup across both tables,
  orphan sweep
- atomic write: behaviour at each crash point
- drain: success, failure to backoff, album creation, batch cap
- ingest: whitelist skip, dedup skip, no-media skip, queue success
- catch-up cursor: stops at `lastKnown`, stall detection, page cap
- alerting: cooldown and rate limiting

Two integration tests encoding the real incidents:

- **Gap B regression**: a fake Immich fails N times then succeeds; assert the
  photo still lands. Proves an Immich outage cannot lose data.
- **Crash regression**: interrupt between file write and row insert; assert
  nothing is lost and the orphan is swept.

All 69 existing tests must stay green.

## Risks and limits

| Risk | Mitigation |
|---|---|
| WhatsApp media expiry / rotated sender keys | Unavoidable. Counted and reported; zip fallback covers the remainder |
| Catch-up request volume on a shared account | Paced at 10s, page-capped, stall-detected |
| Outbox grows during a long Immich outage | Depth and age thresholds raise an alert |
| Alert cannot send when WhatsApp is down | Healthcheck heartbeat backstop |
| `openAsBlob` behaviour with very large videos | Covered by drain tests; falls back to buffered read if problematic |
