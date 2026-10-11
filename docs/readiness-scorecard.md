# Recovery evidence and readiness scorecard (OP-499)

The scorecard consumes the native daily, weekly and monthly report contracts.
It never runs recovery commands. The CLI and manual Dagster asset share the pure
`build_scorecard` function. All status decisions use structured checks, explicit
observation time and declared policy; narrative text has no role in readiness.

## Local operator interface

Retain the JSON outputs of daily, weekly and monthly execution. Ingest them while
the corresponding exported manifest is still available:

```sh
recovery-verification consumer.v1.json --scorecard \
  --as-of 2026-10-11T12:00:00+00:00 \
  --daily-evidence daily.json --weekly-evidence weekly.json \
  --monthly-evidence monthly.json --history artifacts/recovery-history.json \
  > artifacts/scorecard.json
```

Each evidence flag accepts repeated paths. Evidence is validated against the
manifest at ingestion. It captures consumer ownership and entrypoint identifiers
alongside the original report, including tested revision, manifest hash/version,
check timestamps, failure categories, duration, restore artifact hashes and
monthly backup source IDs/timestamps. Native contracts do not expose arbitrary
logs or credentials. Native reports accept an optional `provenance` map keyed by
target ID, with `playbooks` and `inventories` (unique relative repository paths)
and `backups` (`source_id`, `kind`, nullable `artifact_sha256` and aware `timestamp`).
`inline_inventory: "localhost"` explicitly identifies a local inline inventory
without inventing a repository inventory path. `kind` distinguishes declared
`recovery_backup` sources from `representative_fixture` test artifacts. Fixture
identities must not collide with declared recovery source IDs.
These identifiers belong to the report's tested revision; undeclared backup IDs,
future timestamps, unsafe paths and contradictory drill snapshots are rejected.
Capture retains declared backup source IDs even when no snapshot was observed.
Each attempt explicitly exposes null unavailable identifiers and an
`unavailable_provenance` list. Monthly snapshot evidence fills observed backup
identifiers and timestamps; an unassociated weekly artifact hash remains in its
native consumer evidence and is never assigned to a guessed backup source.

The coordinated infrastructure/homelab AC4 patches emit preflight configuration
identifiers and weekly execution metadata through their consumer-owned result
interfaces. Consumer invocation binds that metadata to the exact clean checkout
revision. It flows through native checks and consumer evidence into each tier's
attempt, so weekly identifiers do not overwrite daily configuration identifiers.
Representative fixture hashes must match actual native restore-check evidence.
Production backup IDs retain null unobserved artifact/timestamp fields; fixture
creation times are also explicitly unavailable. Git checkout mtime and verification
time are never substituted for a backup creation time. Local fixture success
does not prove recovery of a declared production backup.

`--history` explicitly creates or atomically updates a local private archive after
successful validation. The archive contains `history` and its canonical SHA-256
`history_reference`. Reads reject schema errors and checksum mismatch. Writes
take an advisory lock, merge all retained attempts, deduplicate exact repeats,
fsync the private temporary file, replace atomically and fsync its directory.
An interrupted write preserves the preceding complete archive; a stale writer
cannot erase another writer's records. Checksums detect corruption, not malicious
rewrites by an actor with archive write access. Store it on operator-managed
persistent storage and back it up; no database or Drive event store is introduced.
Reads and writes are limited to ten MiB and fail explicitly when capacity is
exceeded. Rotate to separately retained archives before reaching the limit;
`--evidence retained-history.json` combines archives without deleting originals.
Different consumer manifests can append their own reports to the same history;
the manifest used to assess an environment must contain its complete target set.
Reports bound to a different manifest are retained as superseded evidence and
cannot establish current assurance. To obtain current aggregate assurance, export
the combined target manifest before running the tier jobs against it.

Read retained evidence without additional execution, or request readable output:

```sh
recovery-verification environment.v1.json --scorecard \
  --as-of 2026-10-11T12:00:00+00:00 --history artifacts/recovery-history.json
recovery-verification environment.v1.json --scorecard \
  --as-of 2026-10-11T12:00:00+00:00 --history artifacts/recovery-history.json --human
```

For identical manifests, history, policy and observation time, JSON is identical
regardless of ingestion order. Targets and diagnostics are sorted, and evidence
references identify canonical captured reports by SHA-256. Latest attempted and
last successful executions remain separate for each tier. Historical success
timestamps survive a later failure, expiry or revision change; they do not upgrade
current readiness. Embedded daily checks retain their own execution timestamps.
Monthly weekly fallback can supply weekly evidence, never drill success.
Each tier also exposes `historical_failure_count`, `consecutive_failures`,
`recurring_failed_checks` and `regression` (a failed latest execution following a
successful one). These describe retained execution history, independently of
current freshness. Unavailable/interrupted attempts break consecutive failures
without erasing previous successful evidence. History is never automatically pruned.

Exit codes are 0 READY, 1 NOT_READY, 3 DEGRADED, 4 UNKNOWN (or event delivery
unavailable), and 2 invalid input. Invalid inputs return a sanitized error with
operator guidance and do not replace an existing archive. Unsupported schema,
duplicate JSON keys, conflicting attempts at the same target/tier/timestamp,
incomplete required check sets, inconsistent timestamps/revisions/provenance,
and fabricated unsupported-tier success fail closed.

## Readiness and assurance

Check vocabulary remains `passed`, `failed`, `unavailable`, `skipped`, and
`not_applicable`; the target vocabulary is READY, DEGRADED, NOT_READY and UNKNOWN.
Each tier separately reports passed, failed, unavailable, missing, expired,
superseded or unsupported, with actionable diagnostics.

- NOT_READY: a current required check failed, or a required upstream target is
  NOT_READY. Cleanup failure is a required recovery failure.
- UNKNOWN: current verification is uncertain, skipped, expired or superseded;
  an upstream target is unknown/missing; or no tier has current successful evidence.
- DEGRADED: some tiers are verified, while other tiers are missing or unsupported.
- READY: all three tiers have current affirmative evidence and all required
  recovery dependencies are READY.

Precedence is NOT_READY, UNKNOWN, DEGRADED, READY for both dependencies and the
aggregate environment. `full_recovery_verified` requires READY.
`strongest_successful_level` reports historical successful execution separately
from `strongest_current_level`. Neither upgrades missing higher-tier evidence.

Illustrative healthy lower-tier evidence for the current consumers yields:

| Consumer / target | Daily | Weekly | Monthly | Readiness | Full recovery |
| --- | --- | --- | --- | --- | --- |
| infrastructure / infrastructure-dev | passed | passed | unsupported | DEGRADED | false |
| homelab / homelab | passed | passed | unsupported | DEGRADED | false |

Both require `add_approved_isolated_bootstrap`; weekly success cannot remove
`isolated_bootstrap_not_supported`. These examples are conditional, not live
runtime attestations. Unavailable external checks instead produce UNKNOWN.

Default evidence ages are one day, seven days and thirty days. Existing target
`cadence_overrides` take precedence. `--policy policy.json` accepts explicit
positive `readiness_max_age_seconds`, `verification_max_age_seconds`,
`drill_max_age_seconds` and `upstream_targets` (target ID to required target IDs).
No hidden environment policy or compatibility loaders are used. Repository
dependencies referencing another manifest consumer through `git://owner/repo`
are also mapped to its recovery targets. Self-repository dependencies remain
ordinary daily repository checks. Unknown explicit upstream IDs prevent READY;
duplicate mappings, unknown downstream IDs and cycles are rejected.

The manual `recovery_scorecard` Dagster job accepts `manifest_json`,
`history_path`, `observed_at` and optional `policy_json` in
`recovery_readiness_scorecard` asset configuration. Its output is the same JSON.
It has no schedule, live event transport or recovery execution side effects.

## Standard event routing

`--notify --previous-scorecard previous.json` enables an explicit publish through
the standardized events-service broker's `/v1/events` interface. Routine successful
runs, the initial nonfailed observation and unchanged snapshots are quiet.
A new current required verification failure or material readiness/assurance/
diagnostic transition emits one event per target, including the relevant tier,
normalized state, failed checks/reasons, last relevant success, tested commit and
evidence reference. Simultaneous failed tiers appear in `failed_verifications`,
each with its checks/reasons, tested commit, relevant success and evidence reference.
Comparing an acknowledged previous scorecard suppresses
repeated failure alerts; a new failed attempt can alert again. Recovery transitions
also emit an event. Retain the previous acknowledged snapshot until publishing
succeeds. Replay the exact current and previous snapshots, including `--as-of`,
after delivery failure. Failure identifiers bind all new failed evidence; transition
identifiers also bind the comparison boundary and observation time. Thus the same
ID always has the same envelope, and separate material transitions get distinct IDs.
A previous observation later than the current observation is rejected.

Inject `RECOVERY_EVENTS_URL` (broker base URL) and `RECOVERY_EVENTS_TOKEN` only into
the process. The credential must be source-bound to `recovery-verification` with
`events:write`. HTTPS is required except for local loopback tests. Transport has a
five-second timeout per attempt, at most three attempts and delays of 0.1 and 0.2
seconds (at most 15.3 seconds plus bounded processing). Only connection failures,
HTTP 429 and 500/502/503/504 retry. Authentication/authorization, conflict, other
permanent errors and malformed receipts fail immediately. The acknowledgement
must contain the exact event and correlation IDs, an aware accepted timestamp
and a duplicate flag. Requests reuse identical bytes and idempotency identity.
Responses are bounded to 64 KiB, with no proxy inheritance or redirects and
sanitized failures. Large diagnostics are bounded to the broker's notification
message contract, explicitly marked `details_omitted`, and retain evidence
references for complete details. Known injected secret values are rejected from evidence and
output, including JSON-escaped representations; unknown credential fields are
rejected by native schemas. Operators must never submit unknown secret values
as identifiers. Manifest arguments, secret references and runtime configuration
are not included in the scorecard.

Configure routing in the existing events-service notification policy for source
`recovery-verification` and event types `recovery.verification-failed` and
`recovery.readiness-changed`. The existing broker-to-notify relay owns channel
delivery. No provider adapter, notification service or deployment change is added.
Broker acknowledgement does not prove channel delivery. Delivery errors leave
readiness JSON intact, emit `notification_delivery_unavailable` on stderr and
return 4. No live events are sent by tests or default scorecard execution.

Production scheduling, persistent mounts, producer credential provisioning,
routing activation and deployment remain OP-500 work.

## Local acceptance checks and release boundary

`wood repo validate --json` runs the complete declared quality workflow, including
branch coverage >=90%. `wood repo verify --json` requires explicit consumer and
events-service source checkouts:

```sh
RECOVERY_CONSUMER_CHECKOUTS='["/path/to/infrastructure","/path/to/homelab"]' \
RECOVERY_EVENTS_CHECKOUT=/path/to/events-service wood repo verify --json
```

The consumer verifier uses disposable local fixture checkouts and resources;
it does not perform production recovery. The events verifier loads the actual v1
envelope/receipt and event-to-notification router, validating routing, payload
compatibility and suppression. Missing schema checkout fails required verification.
Live source credentials, broker persistence/channel delivery and production
change-triggered end-to-end verification remain OP-500 operational acceptance.

Story PRs must target an approved `release/vX.Y.Z`, with validation on both PRs
and release-branch pushes. The current publication caller runs only on main;
Story/release branches and PRs cannot publish. Publication now requires a manual
`workflow_dispatch` on main with `release_approved=true` (default false). Main
pushes cannot publish automatically. Dispatch only after reviewing and approving
the release scope and exact main revision; enabling the flag does not replace
the release readiness/approval process. No publishing workflow is run by tests.
No active release branch/SemVer association was available at this review: keep
the existing Story branch and resolve that gate before delivery. Do not fall back
to a main-targeted Story PR, publish images, or deploy this Story individually.
