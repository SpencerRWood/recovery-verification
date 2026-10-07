# Story #494 platform foundation

## Sources inspected

- OpenProject Story #494 / RV-R1-01; Epic #487 Platform Foundation;
  Initiative #486 Recovery Verification R1; planning version #20 R1, project #3.
- [Requirements](https://docs.google.com/document/d/1jQUP2XLVa2gCCOuksyGBJUtQAj9tRrfZigfIyMPTJ9M/edit),
  last modified 2026-10-04. Story has no predecessors; #495 follows it.
- Published Python/Dagster template main at
  `2b7d901e63095def72171397e3f5b99d457497d3`, used as the local Git base.
- Current local Dagster foundation runtime pins, Docker and smoke contract;
  local template had unrelated uncommitted release changes, left untouched.
- Published shared `release-container.yml@v3`, including its Dagster/PostgreSQL
  candidate gate; v3 tag object `5d498bd760ee9503de19d12fb45ff8e91a3161d7`.
- Infrastructure/homelab ownership guidance and README recovery/deployment
  boundaries. Existing deployment rollback restores configuration and does not
  attest to database restoration. Consumer contracts remain work for #495.

## Requirement allocation

| Story requirement | Foundation implementation |
| --- | --- |
| FR1, FR23 | Strict v1 manifest, schema export, explicit fields and consumer example. |
| FR2 | Host-independent Linux container/gRPC entrypoint and generic Definitions. Deployment belongs to later work. |
| FR9, FR24 | Consumer-owned executable declarations, generic plans, no domain implementation. |
| FR14 | Full manifest validation with optional one/many-target selection. |
| FR15, QR1, QR4 | Pure repeatable contract preflight with no recovery execution. Live prerequisite probes belong to Daily Readiness. |
| QR6 | Deterministic manifest fingerprint, declared revision, check ID and method. |
| QR9 | Separate contract/readiness facts and five normalized check states. |

All eight acceptance criteria are covered by code, documentation and tests.
Standalone repository creation is local pending the delivery approval; remote
repository provisioning, commit/push/PR and CI remain at that boundary.
No consumer repositories or centralized workflow repositories are changed.

## Decisions and limits

JSON is the small language-neutral wire interface; Pydantic provides strict
provider models and a JSON Schema for consumers. Required empty lists express
intent. Unknown fields are rejected to prevent silently embedding domain
configuration or accepting capabilities the provider does not understand.
The revision is declared, not remotely verified in this Story.

Preflight establishes contract validity only. There are no live Git/Infisical/
backup/storage probes, restore resources, cadence jobs, reports, notifications,
historical evidence store, consumer onboarding, or production orchestration.
No application verification contract applies before these capabilities exist.
The candidate-image runtime gate is configured for delivery CI; a local unit
smoke job does not prove image digest, PostgreSQL integration, or deployment.
