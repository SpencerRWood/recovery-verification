# Recovery consumer contract v1

Consumers version-control a JSON manifest described by
[`recovery-manifest.v1.schema.json`](recovery-manifest.v1.schema.json).
[`consumer-manifest.v1.json`](../examples/consumer-manifest.v1.json) is a fictional
example, not a production inventory. Consumers require no provider Python imports.
The provider parser is authoritative for cross-field consistency beyond JSON Schema.

## Fields

All fields below are required, including arrays that may be explicitly empty.
Unknown fields and missing fields fail closed at every nesting level.

| Field | Meaning |
| --- | --- |
| `schema_version` | Exact string `"1"`; unsupported versions are errors, without fallback. |
| `targets` | Nonempty list of targets with unique stable IDs. |
| `id` | Lowercase identifier, unique in the manifest. |
| `owning_repository` | `owner/repository` owning all referenced executable interfaces. |
| `revision` | Exact 40-character lowercase Git commit under test, never a moving branch. |
| `platform` | `linux`; no physical machine model or vendor. |
| `adapter` | `consumer-command-v1`; a declarative interface, not an enabled executor. |
| `entrypoints` | Required `bootstrap` and `recovery` command declarations. |
| `validation_commands` | Nonempty list with unique `id`, nonempty `levels`, and `command`. |
| `required_dependencies` | Unique `id`, `kind`, and `reference` entries; empty means explicitly none. |
| `backup_sources` | Unique `id`, opaque `reference`, positive `max_age_seconds`; may be empty. |
| `allowed_verification_levels` | Nonempty unique subset of `readiness`, `verification`, `drill`. |
| `cadence_overrides` | Unique `level` and positive `interval_seconds` entries; may be empty. |

Every allowed level needs at least one declared validation command. Checks and
cadence overrides cannot name disallowed levels. Levels describe consumer
capabilities for later Stories; declaring a level does not implement or authorize
its execution. Intervals are elapsed seconds; scheduling/timezone policy belongs
to later cadence work.

Each command has `entrypoint`, `args` (explicit argv list), and `timeout_seconds`
(integer 1–3600). Entrypoints are repository-relative executable paths; absolute
paths, traversal, dot segments, shell snippets and embedded source are rejected.
The owning checkout at the declared revision will be the execution boundary.
Arguments and dependency references contain identifiers/options only: never
secret values, credential-bearing URLs, environment dumps, or resolved service
configuration. `secret` dependencies reference Infisical names/paths only.
Dependency kinds are `repository`, `secret`, `artifact`, `storage`, `tool`, and
`runbook`. Artifact references should identify immutable inputs, such as digests.
The provider does not resolve these references in this foundation.

## Provider/consumer boundary

The provider accepts declarations and produces validation plans/evidence. It
contains no target-specific provisioning, recovery, restore, rollback, service
configuration, or repository-name switches. Consumer executables remain in their
own repositories. `validation_plan` preserves owner, revision, and command;
it never executes. It rejects requested levels the target has not allowed.

No executable runner is provided in v1 foundation. Before adding one, later
Stories must verify checkout revision, executable containment including symlinks,
isolation, explicit safe mode, timeout/retry/cleanup, and secret handling. A
manifest is a declaration, not trusted authorization to execute arbitrary code.
Target-specific logic must be added to consumers, not to Dagster definitions.

## Preflight and evidence

Preflight parses the entire manifest before optional target filtering. Unknown or
duplicate selected IDs fail; an invalid unselected target cannot be hidden.
The pure function performs no network access, subprocess execution, filesystem
mutation, or restore operations. The CLI only reads the explicitly named file.
Repeated runs against the same contents produce identical evidence with:

- SHA-256 of the entire parsed manifest serialized by the v1 model, independent
  of whitespace/object-key order (array order is significant).
- Target ID and exact declared consumer revision.
- Check ID `manifest_contract`, method `contract-schema-v1`, explicit result.

Contract validity says nothing about executable availability or recoverability.
Preflight's recovery readiness is always `unavailable`. Errors are sanitized and
never echo submitted values. No secret values should be submitted in the first
place; Dagster persists run configuration in its normal storage.

Reusable check states are `passed`, `failed`, `unavailable`, `skipped`, and
`not_applicable`. Normalization prioritizes failed, then unavailable, then skipped;
only affirmative applicable checks pass. Empty observations are unavailable and
all-not-applicable observations remain not applicable. Historical runs, evidence
storage, reporting, and per-cadence freshness are later Stories.

## Evolution

Breaking wire changes require a new schema version and explicit parser support.
No legacy aliases, schema coercion, or silent capability fallback are supported.
Update the exported schema and contract tests together. Generate the schema with:

```sh
uv run python -c 'import json; from recovery_verification.contract import Manifest; print(json.dumps(Manifest.model_json_schema(), indent=2))' > docs/recovery-manifest.v1.schema.json
```
