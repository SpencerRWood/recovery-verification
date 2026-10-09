# Weekly executable verification (OP-497)

`recovery_verification_weekly` selects `recovery_readiness` and its existing
`prerequisites_ready` check, then `recovery_verification` and
`recovery_paths_verified`. The weekly asset consumes the daily report without
rerunning its checks. The standalone `run_weekly` runs that same daily function
once, then calls `extend_weekly`. Daily evidence must match the manifest and target
selection, remain current and be no older than one hour. A diagnostic daily subset
remains UNKNOWN even if the executable weekly checks pass.

## Explicit local execution

Export from a reviewed, clean consumer checkout and grant execution trust:

```sh
consumer/scripts/recovery.py manifest > /tmp/consumer.v1.json
recovery-verification /tmp/consumer.v1.json --weekly --target TARGET \
  --checkout /path/to/consumer > /tmp/weekly-evidence.json
```

`--weekly` requires one explicit target/checkout; it cannot be combined with
daily/check-subset/invoke options. Python callers can use `run_weekly` with explicit
checkout mappings or `invoke_verification(..., trusted=True)` for one diagnostic
consumer command. Only validation commands advertising `verification` execute.
Bootstrap, rollback and drill entrypoints are never selected.

Dagster requires the same non-secret `DailyConfig` for both selected assets:
`manifest_json`, `checkouts`, optional `target_ids`. Do not put credentials into
run config. Weekly scheduling and production activation belong to OP-500; this
Story adds a manually executable job with no enabled weekly schedule.

## Evidence and failure semantics

The verification consumer wire result is strict schema `1`, mode `verification`,
normalized `readiness_state`, and 1–128 unique checks. Each check has `id`, `phase`,
`state`, identifier-only `reason` and optional `artifact_sha256`. Required phases
are `automation_check`, `automation_idempotence`, `restore`, `schema`, `integrity`,
`synthetic`, `cleanup`. These phases cannot be not-applicable; successful restores
require a checksum. `classification` records excluded tasks explicitly.

The provider binds the consumer evidence to the same target, exact source revision,
manifest hash and aggregate observation, with timestamp and duration. It rejects
duplicate keys/IDs, missing phases, arbitrary output fields, contradictory states
and exit-code mismatches. Consumer reasons contain no raw logs, restored contents,
SQL results, credential values or exception messages. Retain the JSON report;
reproduction uses its revision, command ID, phase/reason and immutable backup hash.
No reporting, notifications or durable scorecard are added (OP-499).

Required failures, including cleanup failure or idempotence drift, produce NOT_READY.
Transient dependency outages, missing tools, invocation deadlines and unobserved
cleanup produce UNKNOWN. Cleanup results remain separate from restore results;
a process timeout never invents successful cleanup. All daily uncertainty continues
to constrain the overall result. Source changes after verification supersede the
report and force UNKNOWN. Apply `assess_currency` with an explicit maximum age
before reusing stored evidence; the weekly job itself always creates new evidence.

## Isolation and scope

The adapter retains clean-checkout/origin/revision validation, argv execution,
credential-free environment, 64 KiB output limit and the declared 300-second
consumer deadline. On verification termination it sends SIGTERM and gives the
consumer 15 seconds for cleanup, then kills the process group. Each consumer owns
its shorter operation deadlines, isolated temporary resources, signal cleanup
and explicit safe-playbook policy. There are no retries.

Infrastructure restores a checked-in, checksummed synthetic custom-format
PostgreSQL backup into a new local socket-only cluster/database, verifies schema,
constraints/fixture totals and a transactional insert/read/rollback. Its approved
Ansible playbook recovers the canonical Compose file and renders the real
application provisioning SQL. The SQL runs twice in that temporary cluster and
validates restricted role semantics and absence of reconciliation drift.

Homelab restores a checksummed representative archive into new temporary storage,
rejects traversal, links, special files and undeclared/oversized members, validates
file hashes, and serves the restored fixture on a temporary loopback HTTP port.
Synthetic HTTP requests validate the fixture's health and library data. Its approved
Ansible playbook recovers canonical books Compose and renders the real media mount
readiness template; the script receives syntax validation only, never execution.

Both approved playbooks run check mode, then two isolated applies. Skipped
check-mode tasks fail; missing/malformed recaps fail. Idempotence must be explicitly
safe and the second-run changed count must stay within the reviewed allowance
(zero). Credential resolution, live mounts, service startup and infrastructure
firewall tasks remain explicitly classified as outside this local scope.

These representative checks prove mechanisms and synthetic data usability, not
restorability of every retained production backup or a full application/host rebuild.
The daily checks still reference the independently retained live backup sources;
local fixture success does not mask their freshness or availability failures.
No live database, NAS path, Compose runtime, image publication or deployment is
changed. Full host reconstruction remains OP-498; Release integration is the next
runtime gate.

## Cross-consumer validation

```sh
uv run python scripts/verify-consumers.py --weekly \
  /path/to/infrastructure /path/to/homelab
```

The required repository-owned application check is `weekly-consumer-contracts`.
Set `RECOVERY_CONSUMER_CHECKOUTS` to a non-secret JSON array of the two source
checkout paths and run `wood repo verify --json`. The check uses explicit configured
paths, has a 40-second internal deadline plus adapter cleanup grace, and returns
Wood's source-bound verification record/log paths. Missing configuration fails
directly; there is no default checkout or legacy setting fallback. Retain this
record alongside development validation for later delivery evidence.

This uses disposable clean Git snapshots including uncommitted source changes;
its temporary commits are test fixtures, never delivery commits. It executes both
actual local preflights and weekly contracts through the public provider adapter.
Infrastructure requires PostgreSQL server/client tools (the representative dump
was generated with PostgreSQL 18), and both consumers require Ansible in their
development tool PATH. Full repository tests include failure, timeout, cleanup,
unsupported task, drift, integrity and evidence cases. Missing local integration
tools are reported as skipped tests rather than a passed restore.
