# Monthly isolated recovery drill (OP-498)

`recovery_drill_monthly` selects the shared `recovery_readiness` asset and
`recovery_drill`, with their readiness and isolated recovery checks. It consumes
daily evidence once; the manifest, selection, revisions and one-hour currency must
match. Configure both assets with the same non-secret `DailyConfig`. Scheduling
and runtime activation remain OP-500.

Standalone execution uses the same implementation:

```sh
recovery-verification /tmp/consumer.v1.json --monthly --target TARGET \
  --checkout /path/to/reviewed/clean/consumer > /tmp/monthly-evidence.json
```

An explicit checkout grants execution trust for that consumer revision. Export
the manifest from the consumer's supported `scripts/recovery.py manifest`
interface. The CLI requires one target and checkout and rejects combinations with
daily, weekly, check-subset or readiness-invocation options.

## Capability approval

Every target must now declare `ephemeral_rebuild`; old declarations fail directly.
First-party consumer sources, examples, fixtures and the published schema have
been migrated. No default, compatibility loader or hostname fallback exists.

Supported targets require `supported: true`, an identifier `provider`,
`environment: "disposable-linux"`, an identifier `reason`, the `drill` allowed
level and at least one drill validation command. Unsupported targets declare
`supported: false`, null provider/environment and a specific missing capability.
The declaration must agree with the allowed levels. Production environments,
hostnames and arbitrary inventories are not accepted by this interface.

Infrastructure-dev and homelab explicitly declare
`isolated_bootstrap_not_supported`. Their normal bootstrap currently produces a
plan, and their existing rollback/storage/runtime contracts cannot reconstruct a
blank host in isolation. The monthly tier executes their strongest supported
weekly tier, preserving its phase/artifact/cleanup evidence and the daily checks.
It adds an unavailable monthly capability check. Successful representative weekly
restores therefore leave monthly readiness UNKNOWN; actual failures remain
NOT_READY. This implementation does not claim a real end-to-end rebuild of either
consumer or change their managed hosts.

## Provider execution contract

A supported consumer's drill command owns the approved provider implementation;
the generic invocation adapter does not execute host bootstrap or rollback on the
controller. It requires a trusted clean checkout, matching repository/revision,
bounded output/time and strict evidence. Ambient application credentials are not
inherited. The consumer resolves secrets inside its disposable environment via
the declared standard runtime mechanism. Credentials never enter Dagster config
or evidence.

Consumer authors can use `drill.execute_drill(target, provider)` with a
`LinuxProvider` plugin. The provider name must match the target's approval. The SDK
assigns one random `temporary://` resource ID before provisioning and calls phases
in this order:

1. Provision or obtain a blank, isolated Linux environment.
2. Run `target.entrypoints.bootstrap` using normal bootstrap semantics there.
3. Retrieve required repositories/artifacts at their declared identities.
4. Resolve required secrets with the standard runtime mechanism there.
5. Run `target.entrypoints.recovery` inside that environment.
6. Restore representative state from the declared backup sources.
7. Start workloads using the consumer's normal service startup path.
8. Run deterministic health and application checks.
9. Independently destroy all allocated hosts, containers, volumes, credentials and
   restored state, after success or failure.

Providers must register every allocation against the supplied ID before creating
it, so cleanup can destroy partially provisioned resources. They must enforce
isolation and forbid production connections; declaration/evidence validation is
not a sandbox. Implement a SIGTERM handler that unwinds to cleanup, bound each
operation, and make destruction fit the adapter's 15-second termination grace.
Cleanup may use a separate provider API after the workload environment is gone.
Failure to observe cleanup is reported separately as unavailable, never passed.
The SDK stops subsequent recovery phases at the first non-passing result, always
attempts cleanup, sanitizes unexpected plugin errors and performs no retries.
There is no installed provider for the current unsupported consumers. Unit test
providers prove orchestration behavior, not a real Linux rebuild.

## Evidence

Mode `drill` records the approved provider/environment, temporary resource ID,
start/end timestamps, monotonic duration, ordered phase checks and restored backup
timestamps/content hashes. The adapter requires snapshot source identities to
match the manifest on successful restore. Timestamps must be timezone-aware,
non-future and no later than drill start. Monthly checks compare the snapshots
actually used with each source's declared maximum age; stale state fails readiness
even if workloads start successfully. These observations support later RTO/RPO
analysis without asserting that a recovery objective has been met.

Incomplete phases, inconsistent readiness, execution after an earlier failure,
missing cleanup, wrong provider, unknown backup identities or arbitrary extra
fields are rejected. A timeout/malformed result includes separate unobserved
cleanup evidence. Final checkout changes supersede the report. `drill_evidence`
and `fallback_evidence` are bound to the corresponding observations.

## Verification

`wood repo validate --json` runs the declared development checks. The required
application check is now `monthly-consumer-contracts`; it includes the previous
weekly representative recovery checks and validates the current explicit monthly
capability. Set `RECOVERY_CONSUMER_CHECKOUTS` to a non-secret JSON array of the two
consumer source paths, then run `wood repo verify --json`. The existing verifier
creates clean disposable Git snapshots of those sources, including uncommitted
changes. Its temporary test commits are fixtures, not delivery commits. It invokes
drill commands only when a consumer explicitly declares support. Passing this
application check validates the declared scope; unsupported consumers still have
no full-drill evidence. Missing configuration or tools fail directly.
