# Recovery Verification operations (OP-500)

Implementation, integration, activation, and production recovery assurance are
separate states. This Story prepares the existing Dagster control plane; it does
not authorize deployment, schedule activation, live alerts, publication, or
production restoration. Jobs and the change sensor are **STOPPED** by default.
`release/v0.6.0` is the R1 integration target; all seven R1 Stories remain in scope.

## Deployment configuration

Set `RECOVERY_ORCHESTRATION_CONFIG` to a reviewed, non-secret JSON file conforming
to `OrchestrationConfig` in `orchestration.py`. Required fields are
`manifest_json` (the merged, exported v1 manifest), `checkouts` (target ID to
absolute clean Git checkout path), `state_directory` (absolute durable writable
directory), and `environment` (operator environment identifier). Optional fields
are `cadence`, `manifest_exports`, `isolated_test_mode`, `notifications_enabled`,
`policy`, and `probe_attempts`. Unknown fields, invalid cron, unsafe identities,
relative paths, mismatched mappings and secret values fail closed. No legacy
configuration fallback remains.

Export each initial consumer manifest using its existing
`scripts/recovery.py manifest`; combine the `targets` under schema_version 1.
For current-manifest refresh, declare each consumer's export command explicitly:
`{"entrypoint":"scripts/recovery.py","args":["manifest"],"timeout_seconds":10}`.
The generic exporter validates identity, limits output and execution, and carries
the actual clean checkout revision. It never rewrites an old report's revision.
It does not accept an arbitrary shell command or inherit application credentials.

The default cadence is deployment data in packaged `cadence.json`: daily 06:00,
weekly Sunday 07:00, monthly day 1 at 08:00, all **America/New_York**. Replace
`cadence.daily`, `cadence.weekly`, and `cadence.monthly` cron strings in the reviewed
configuration and reload the code location to change times. DST is handled by
Dagster's timezone-aware scheduler. Missing configuration skips ticks; invalid
configured cadence fails location loading, rather than silently using other times.

The three jobs select the existing shared assets and checks:
`recovery_readiness_daily`, `recovery_verification_weekly`, and
`recovery_drill_monthly`. Every scheduled request has an environment/tier/time
execution key and a 3600-second Dagster maximum runtime tag. Infrastructure's
existing run monitoring must remain enabled. The instance's existing PostgreSQL
storage retains schedules, sensor cursor, run IDs, events and execution history;
no second database is introduced. Archive and execution-state directories must
also survive code-server restart and image replacement.

## Execution, change triggers and retries

After separate activation approval, targeted manual execution uses the same
resource, lock, archive and outbox as scheduled runs:

```sh
recovery-verification-run readiness --target infrastructure
recovery-verification-run verification --target homelab
recovery-verification-run drill --target homelab
```

Provide `--execution-key <previous-key>` only to resume that exact manifest/run;
a new key starts a new observation. A cached completed report is not re-executed.
Successful Dagster execution means evidence was collected, not that readiness is
READY. Read the persisted scorecard and journal. Direct diagnostic CLI commands
remain available, but do not run standalone restore diagnostics against a runtime
checkout concurrently; use these locked operational jobs for attended execution.

The stopped `recovery_relevant_changes` sensor polls configured clean checkout
revisions every 60 seconds. The first tick establishes a baseline, subsequent
ticks diff exact commits and emit deterministic run keys for affected targets.
Configuration formats (YAML, JSON, TOML, INI, CFG, CONF and ENV), Dockerfiles,
inventories, Compose, environment selections, mounts and secret references select
readiness, including backup paths and recovery manifests. Roles, playbooks and
recovery/backup/restore implementation entrypoints select weekly verification.
Bootstrap changes select monthly only when `isolated_test_mode` is explicitly
true; otherwise they remain weekly. Rules use generic path components and formats,
independent of consumer identity. Only passive documentation/media outside these
surfaces is ignored; unknown formats conservatively select readiness. The strongest
tier wins across all changed paths. Renames include both old and new paths.
Failed classification, export or diff leaves the cursor unchanged for the next tick.
Dagster's durable cursor and run-key suppression prevent replayed change ticks.

The deployment operator must supply and advance the clean, consumer-owned source
checkouts to reviewed immutable consumer revisions; this sensor does not deploy
consumers or fetch arbitrary unreviewed branches. Advance checkouts only between
operations, using the shared operation lock. A changed checkout during a run
fails revision checks and cannot be reported as verified. Mount independent Git
clones with valid in-container metadata, not host worktrees whose `.git` files
point outside the mount. UID 10001 must be able to read/trust those checkouts.

One nonblocking filesystem lock covers the entire multi-asset run, all tiers and
all consumers, including notification persistence. A competing run fails with
`recovery_operation_already_running`; it does not execute a conflicting restore.
Each dependency probe has the existing 30-second worker deadline and may retry
only `unavailable` infrastructure observations, twice by default (maximum three),
with a 0.2-second delay. Confirmed recovery failures are not retried. Consumer
commands retain their existing execution and bounded cleanup policies; no blind
consumer restore retry is added. For an interrupted attempt, inspect/reconcile
consumer cleanup before explicitly resuming. Hard-kill cleanup is never assumed.

`history.json` retains OP-499's checksummed append-only logical evidence and
atomic/fsynced writes. `execution.json` records attempt start/end, state, native
reports, prior scorecard, notification outbox and acknowledgments; it is also
checksummed and atomically written with mode 0600. A restart marks abandoned
running attempts interrupted and preserves all earlier successful evidence.
Missing requested tier output remains an interrupted attempt, never success.
Previous evidence is assessed for age/current revision through the existing
scorecard. Archive/journal limits fail closed; do not silently prune history.

## Notifications and evidence

Notifications are disabled by default. After separate approval, set
`notifications_enabled=true` and inject `RECOVERY_EVENTS_URL` and
`RECOVERY_EVENTS_TOKEN` through the existing Infisical runtime mechanism. The
source-bound broker token, Infisical token and database password belong only in
the protected process environment, never Dagster config or reports.

OP-499's event schema, fixed reason diagnostics, bounded transport, receipt
validation and idempotency remain authoritative. Operational delivery adds the
environment field and a durable outbox. Events are written before transmission;
acknowledged events are removed atomically. Interrupted acknowledgment replay
uses the same event/correlation/idempotency identifiers and the broker's duplicate
receipt. Three bounded attempts with five-second HTTP timeouts and 0.1/0.2-second
delays remain the transport policy. Pending events retry on the next operational
run. Unchanged readiness/failure conditions stay quiet; changed failure/recovery
conditions alert. Delivery failure remains `notification_state=unavailable` in
the journal and never rewrites recovery readiness. Do not log raw HTTP bodies,
commands, inventory output, tokens or restored backup contents.

Build operator scorecards from the actual retained archive with the established
CLI (`--scorecard --history ... --as-of <aware timestamp>`, optionally `--human`).
Daily/weekly/monthly attempts and last success, evidence age, source commits,
backup/restore provenance, recurring failures and dependencies remain independent.
Operational archives are separate from disposable test archives. Unit/integration
simulations and representative fixture checks are labeled in their test/contract
records and must not be substituted for real backup or production recovery proof.

Both current consumers declare isolated bootstrap unsupported. Monthly executes
their strongest supported weekly mechanism and records the missing capability.
Monthly status remains unsupported and full recovery remains false. FR20 and
OP-500 explicitly allow this strongest-supported result; a new reconstruction
provider is not required to turn the implementation tests green. Actual daily
success additionally requires current real backups, accessible mounted storage,
resolvable metadata-only secret references and retrievable deployment artifacts.

## Activation, rollback and future consumers

Infrastructure owns the optional independently versioned component, private gRPC
network, digest-qualified image selection, existing Dagster workspace integration,
health gate, scoped secret resolution and durable bind mounts. Both environment
selectors remain false. A separately reviewed activation must select a published
image digest, reviewed configuration, clean source clones, storage paths, scoped
secret mapping, shared Dagster database/runtime inputs, and retention capacity.
The image includes Ansible and PostgreSQL restore tools for temporary local
resources; it receives no Docker socket or write access to source/NAS backups.

`recovery-verification-health` verifies all three jobs, New York schedules and
the change sensor locally; the container health check also probes gRPC. Real
production registration and executed schedule/notification health still require
evidence after activation. The existing manual publication/deployment gates and
immutable shared workflow SHA must remain intact.

Before activation, record the previous immutable image, configuration, workspace,
schedule/sensor state and archive backups. On failure, stop the three schedules and
sensor, restore the previous approved component/configuration release through the
existing infrastructure rollback path, and retain evidence/state. Do not restore
production databases or discard journals as a configuration rollback. Verify the
previous location and that only one daemon operates before resuming cadence.

Future consumers export the same v1 contract, own their recovery implementation,
declare capability/backup/dependency/cadence metadata, supply a clean revision and
explicit export mapping, and pass provider/consumer contracts. Add no generic
orchestrator branches for a consumer name. Enable isolated monthly tests only for
an explicitly declared disposable environment and separately approved test mode.
