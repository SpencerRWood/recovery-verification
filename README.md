# Recovery Verification

Standalone Python/Dagster provider for a versioned recovery consumer contract.
It validates declarations, runs daily prerequisite checks and orchestrates isolated
weekly executable recovery verification.
Consumers own bootstrap, provisioning, restore, rollback, and service validation.
Retained daily, weekly and monthly reports feed a deterministic operator scorecard;
see [readiness scorecards](docs/readiness-scorecard.md) for history, freshness,
dependency precedence, JSON/human output and standardized event routing.

## Local development

Requires Python 3.14 and uv. From the Story checkout:

```sh
uv sync --frozen --group dev
uv run recovery-verification examples/consumer-manifest.v1.json
uv run recovery-verification examples/consumer-manifest.v1.json --target example-linux
uv run dagster dev -m recovery_verification.dagster.definitions
wood repo validate --json
uv build
```

Without execution options, the CLI exits 0 for a valid manifest/selection and 2
for invalid or unreadable input. Contract-only success does not prove readiness.
The example is fictional; infrastructure and homelab export their own manifests.
See [daily readiness](docs/daily-readiness.md) for probes, explicit trust,
runtime inputs, evidence freshness and independent check diagnosis.

`contract_preflight_job` accepts explicit `manifest_json` and optional `target_ids`
in its op config. It uses the same pure validator as the CLI. `runtime_smoke_job`
provides the platform's fast infrastructure-only smoke check.
`recovery_readiness_daily` uses the shared readiness asset and prerequisite asset
check. Its UTC schedule is stopped by default until explicitly configured and
enabled. `recovery_verification_weekly` selects the same daily asset/check and adds
consumer-owned executable checks. See [weekly verification](docs/weekly-verification.md).
`recovery_drill_monthly` adds isolated drills or explicitly unsupported weekly
fallback. `recovery_scorecard` assesses retained evidence manually.
Production orchestration remains OP-500 work.

## Structure and contracts

- `contract.py`: strict immutable wire models and parser.
- `models.py`: checks, evidence, and conservative normalized readiness.
- `preflight.py`: deterministic validation evidence and target selection.
- `readiness.py`: shared daily checks and revision-bound readiness evidence.
- `weekly.py`: daily evidence reuse and generic executable verification.
- `probes.py`: bounded read-only prerequisite probes with sanitized output.
- `adapters.py`: declarative consumer validation plans; no executor.
- `dagster/definitions.py`: generic code location.
- [Consumer contract](docs/recovery-contract.md): versioning, ownership, safety.
- [Consumer onboarding](docs/consumer-onboarding.md): source references, trusted
  readiness invocation and cross-consumer validation.
- [Implementation scope](docs/platform-foundation.md): requirements and provenance.

Consumers exchange JSON and implement their own executable entrypoints. They do
not need this package as a runtime dependency. No target-specific dispatch,
Ansible roles, Compose files, restore scripts, or secret values belong here.

## Runtime and release

```sh
docker build -t recovery-verification .
docker run --rm -e DAGSTER_GRPC_PORT=4000 -p 4000:4000 recovery-verification
```

The container serves `recovery_verification.dagster.definitions` over gRPC as
non-root. `DAGSTER_GRPC_PORT` is required. Infrastructure owns deployment,
workspace registration, host selection, environment injection, and secrets.
The code location has no hardcoded physical host, network, or service address.

Dagster 1.13.24, dagster-postgres 0.29.24, SQLAlchemy 2.0.54, and
psycopg2-binary 2.9.13 form the pinned platform runtime family. Review their
updates together. Validation and release consume immutable revisions of
`SpencerRWood/workflows`, with each caller's helper pin matching its workflow pin.
Release uses the approved publication safeguards integrated in Workflows PR #38.
The Dagster contract enables
exact-candidate PostgreSQL/gRPC smoke validation before release publication.
Semantic-release owns versions and tags.

Publication requires a separately approved manual dispatch on main with
`release_approved: true`, the exact reviewed current main SHA in
`approved_revision`, and an explicit `recovery_approved` choice (default false).
Recovery authorization covers only the separately reviewed candidate reuse or
partial publication steps; a rerun does not grant it. The shared workflow verifies
source, signed prepared-input identity, platform, package version and registry
digest before promotion and finalization. Retain the reviewed sole main/GHCR
writer controls throughout publication, as described in the
[shared publication runbook](https://github.com/SpencerRWood/workflows/blob/74b31b0d4f9f39dfe6a3a0f3c1bbeffca8bfaee4/docs/publication-safeguards.md).
A caller update requires fresh exact-source review and nonpublishing version
verification; the planned v0.6.0 is not publication approval. CI, caller merges and
publication never authorize deployment or schedule/sensor activation.

Quality gates are Ruff, Ruff formatting, strict mypy, pytest with branch coverage
(minimum 90%), pre-commit, and Python package build. Wood retains validation logs
and the source-bound record used during delivery. `wood repo verify --json` runs
the declared monthly consumer-contract check with explicitly configured consumer
checkouts; production cadence and deployment acceptance belong to OP-500.
