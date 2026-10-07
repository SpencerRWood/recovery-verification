# Recovery Verification

Standalone Python/Dagster provider for a versioned recovery consumer contract.
This foundation validates declarations without executing recovery operations.
Consumers own bootstrap, provisioning, restore, rollback, and service validation.

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

The CLI exits 0 for a valid manifest/selection and 2 for invalid or unreadable
input. Its JSON distinguishes `contract_state: passed` from
`readiness_state: unavailable`. No external prerequisites, backups, secrets,
entrypoints, or recovery capabilities are proven by this foundation. The example
is fictional; infrastructure and homelab onboarding belongs to Story #495.

`contract_preflight_job` accepts explicit `manifest_json` and optional `target_ids`
in its op config. It uses the same pure validator as the CLI. `runtime_smoke_job`
provides the platform's fast infrastructure-only smoke check. There are no
schedules, sensors, daily/weekly/monthly recovery jobs, or production execution
adapters in this Story.

## Structure and contracts

- `contract.py`: strict immutable wire models and parser.
- `models.py`: checks, evidence, and conservative normalized readiness.
- `preflight.py`: deterministic validation evidence and target selection.
- `adapters.py`: declarative consumer validation plans; no executor.
- `dagster/definitions.py`: generic code location.
- [Consumer contract](docs/recovery-contract.md): versioning, ownership, safety.
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
updates together. Validation consumes `SpencerRWood/workflows` at `v3`; release
consumes its integrated `release-container.yml@v3`. The Dagster contract enables
exact-candidate PostgreSQL/gRPC smoke validation before release publication.
Semantic-release owns versions and tags. Publishing, CI and deployment have not
been performed during local implementation.

Quality gates are Ruff, Ruff formatting, strict mypy, pytest with branch coverage
(minimum 90%), pre-commit, and Python package build. Wood retains validation logs
and the source-bound record used during delivery. Application runtime verification
is deferred until execution capabilities exist; no `[tool.wood.verify]` contract
is declared for this declaration-only foundation.
