# Daily recovery readiness (OP-496)

The generic `run_readiness` function powers both standalone diagnosis and the
Dagster `recovery_readiness` asset. It validates the whole manifest before
selection, requires an explicitly trusted clean checkout with matching owner and
exact revision, checks executable presence, invokes only declared readiness
commands, and probes declared dependencies and backups. It rechecks source
identity after execution. It never runs bootstrap, recovery, restore, restart,
check-mode or idempotence operations.

## Standalone diagnosis

Export the manifest from a reviewed clean consumer checkout:

```sh
/path/to/consumer/scripts/recovery.py manifest > /tmp/consumer.v1.json
recovery-verification /tmp/consumer.v1.json --daily --target TARGET \
  --checkout /path/to/consumer
recovery-verification /tmp/consumer.v1.json --daily --target TARGET \
  --checkout /path/to/consumer --check consumer_consumer_preflight
```

`--checkout` with `--daily` explicitly trusts that target's readiness commands.
Target and check options select independent diagnostic runs. Omitted checks are
recorded as skipped; a subset cannot claim READY. Exit codes are 0 READY,
1 NOT_READY, 4 UNKNOWN, and 2 invalid input. JSON is the evidence output; redirect
it to retain a report. No automatic evidence store is introduced here.

## Checks and runtime inputs

Consumer `preflight --local-only` validates Ansible inventory parsing, syntax,
required configuration, roles and entrypoints. It discards raw command output and
receives no application credentials. Overall consumer preflight without this
scope flag continues to disclose unavailable external prerequisites.

The provider resolves `git://owner/repository` with a bounded remote Git query.
It expands images from `repo://` YAML catalogs and checks OCI registry manifests,
including anonymous bearer challenges. Confirmed missing artifacts fail;
registry outages and unresolved image templates remain unavailable. Tags attest
only to availability at observation time, not immutable artifact identity.

Secret probes use Infisical metadata with `viewSecretValue=false` and reference
expansion disabled. Inject `RECOVERY_INFISICAL_URL` and
`RECOVERY_INFISICAL_TOKEN` into the process; never put tokens in configuration,
manifests, CLI arguments or evidence. `RECOVERY_INFISICAL_PROJECTS` is a JSON map
from declared project names to UUIDs. Missing configuration or denied metadata
access means unavailable; a successful metadata listing without a required key
fails. Unexpected secret values are rejected without logging the response.

Storage probes require `RECOVERY_LOCAL_STORAGE=1`, an explicit assertion that the
worker sees the target's storage namespace. They check declared `path:///` and
`mount:///` references, readability and mount presence. Backup checks require a
nonempty file and a modification time within the declared freshness limit.
A directory alone cannot attest to a fresh backup and remains unavailable.
Freshness does not attest to restorability; deeper verification belongs to later
Stories. Runbooks are checked for presence in the source checkout.

Each external dependency probe runs in an isolated worker with a 30-second
deadline, bounded output and process-group cleanup; HTTP calls have a five-second
timeout and one MiB response limit. No redirects or inherited proxy configuration
are used. Results contain fixed reason identifiers, never raw output, response
bodies, secret values or exception text. Total run duration scales with the
declared check count and consumer command deadlines; probes are not retried.

## Evidence and readiness

Each observation contains target/check IDs, timestamp, exact tested revision,
duration, state and failure category. Confirmed prerequisite failures produce
NOT_READY. Verification infrastructure uncertainty, unconfigured probes, stale
checkouts or skipped checks produce UNKNOWN. READY requires applicable checks
to pass with no failures or uncertainty.

`assess_currency(report, manifest, now=..., max_age_seconds=...)` checks both
observation and report ages against an explicit policy. A changed manifest or
consumer revision marks evidence superseded; expired or future observations are
stale. Both force UNKNOWN. Stored evidence must be reassessed before being used
as a current readiness claim. Durable scorecards and notification policy belong
to OP-499.

## Dagster

`recovery_readiness_daily` selects the shared asset and its
`prerequisites_ready` check. The check passes only for READY and records the
structured report in metadata. UNKNOWN and NOT_READY fail the check while still
preserving observations; successful asset execution alone does not imply READY.

The schedule runs at 06:00 UTC and is stopped by default. Set
`RECOVERY_READINESS_CONFIG` to a non-secret JSON file containing `manifest_json`,
`checkouts` (target-to-path map), and optional `target_ids`/`check_ids`. Missing or
invalid configuration skips the tick. Do not put secret values in Dagster config:
Dagster persists it. Production registration, schedule activation and restart-safe
orchestration are handled in OP-500.
