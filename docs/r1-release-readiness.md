# R1 / planned v0.6.0 readiness — OP-500 implementation review

**NO-GO for release activation or production recovery assurance.** This report
separates code implementation, source integration, runtime activation and actual
production evidence. It is not a release/deployment approval.

Authoritative sources: [R1 workbook](https://docs.google.com/spreadsheets/d/1zovdZoppOrTdlbqcardk-IbSmxsuL_J3VXyzzP1pDM4/edit),
[requirements](https://docs.google.com/document/d/1jQUP2XLVa2gCCOuksyGBJUtQAj9tRrfZigfIyMPTJ9M/edit),
[OP-500](https://projects.woodhost.cloud/work_packages/500), and
[OP-499 integrated acceptance review](https://drive.google.com/file/d/1MSTu84DowghdBarG67YEvrxffTAE5kUN/view).
All seven RV-R1-01–07 Stories remain Release 20 scope. Planned integration branch
is release/v0.6.0; semantic-release owns the eventual actual published version.

## OP-500 acceptance disposition

| AC | Status | Executable evidence / outstanding requirement |
| --- | --- | --- |
| 1 | PASS locally | Three exact named jobs retain shared readiness/weekly/monthly assets/checks; real Definitions loading and execution tests. |
| 2 | PASS locally | Three stopped schedules, America/New_York, configurable deployment cron, invalid-config failure and schedule-tick tests. |
| 3 | PASS locally | Commit-bound change sensor, durable cursor, affected-target requests and recovery-path classification tests. Runtime checkout advancement/activation remains required. |
| 4 | PASS locally | Proportional tiers, explicit isolated mode, monthly opt-in refusal test, preserved consumer capability/isolation contract. |
| 5 | PASS locally | Run-wide exclusion, checksummed durable journal/archive, interrupted attempts, repeated-run caching, prior-success preservation, transient-only probe retry and broker outbox tests. |
| 6 | BLOCKED | Optional infrastructure component/configuration is prepared and disabled; independently versioned published digest, reviewed runtime configuration and separately authorized deployment are absent. |
| 7 | BLOCKED | Local registration health contract passes; production code-location health/jobs/schedules have not been activated or observed. |
| 8 | BLOCKED | Real safe consumer preflight, representative weekly restores and actual monthly fallback produce provenance and honest UNKNOWN/unsupported scorecards. Daily operational success, live notifications and activated change-triggered execution remain unobserved. Simulation tests are separate evidence. |
| 9 | PASS locally | Operational runbook covers manual targets, history/readiness, retry/cleanup, notifications, rollback, configuration and generic future-consumer onboarding. |

Local PASS does not imply hosted CI or integrated OP-500 changes. Keep OP-500
In Progress until all nine criteria, designated-target integration and actual
runtime acceptance have verifiable evidence. Never substitute Closed predecessors
or a successful asset execution for production health/recovery proof.

## All seven R1 Stories

| Story / AC set | Implementation / existing integration evidence | Remaining operational qualification |
| --- | --- | --- |
| OP-494 / AC1–8 | Closed; existing foundation models, schema guards, provider/consumer ownership boundary, diagnostic preflight, Definitions and runtime-smoke tests. | Existing code location is an implementation artifact, not an active service. |
| OP-495 / AC1–8 | Closed; both native consumers export revision-bound v1 manifests, deterministic interfaces, required dependency declarations and non-destructive contracts. | Actual storage, credentials, backup and artifact availability must be observed on the execution host. |
| OP-496 / AC1–8 | Closed; shared daily checks and pass/fail/unavailable/skipped semantics, provenance, exact revision and freshness tests. | Both actual local consumer scorecards still have unavailable operational daily evidence. |
| OP-497 / AC1–9 | Closed; approved local Ansible check/idempotence, isolated representative Postgres/file restore, integrity/synthetic/cleanup evidence and failure tests. | These fixtures demonstrate restore mechanisms; they do not establish retained production backup freshness or restoration. |
| OP-498 / AC1–9 | Closed; supported-provider lifecycle tests and actual strongest-supported fallback, duration/provenance and unsupported-isolation classification. AC7 explicitly permits unsupported rebuild fallback. | Infrastructure and homelab lack isolated blank-host bootstrap. Monthly remains unsupported; full recovery false. |
| OP-499 / AC1–10 | Closed/integrated; daily/weekly/monthly independent scorecards, attempt/success history, native provenance, regression integrity and schema-compatible quiet notifications. | Actual broker delivery and production readiness were intentionally not attested. |
| OP-500 / AC1–9 | Local implementation/validation is prepared; exact disposition above. | Integration, independently versioned release artifact, runtime activation and end-to-end acceptance remain gates. |

The predecessor implementations remain in the approved integration base
`ae9079df04a88a34d0afce1f4b4f2d02eac814f5`. Consumer merged revisions are
infrastructure `5a942ea6161fed73ce0c358514f7b522dac75201` and homelab
`7dadf5d9f201f051e1eb78d49a002484a6f86d0c`. OP-500 adds no duplicate consumer
recovery implementation and does not change their monthly support declarations.

## Actual evidence and unresolved dependencies

Both local native consumer contracts execute their existing representative
restore paths. Fresh monthly fallback reports independently retain unavailable
daily operational checks, passed weekly execution and unsupported monthly
reconstruction. Overall remains **UNKNOWN**, full recovery **false**. Exact times,
tested source/snapshot revisions, hashes, native playbook/inventory identifiers and
raw structured reports are retained in Wood verification logs, separately from
simulated unit/integration outcomes. Proposed infrastructure changes are tested as
an explicit disposable source snapshot; clean merged revisions are verified
separately. Snapshot success is never production recovery evidence.

Runtime gaps include: designated Linux execution host and writable durable
state/instance storage; reviewed clean consumer clones and revision advancement;
current retained backup selection/identity/timestamp and read-only mount access;
retrievable resolved artifacts (including local/variable image catalogs);
metadata-only Infisical credentials/project mapping; source-bound events-service
token and actual broker availability; separately approved schedules, sensor and
live notifications; and evidence-retention capacity. Existing fixed migration
snapshot paths may be stale when observed. Do not rewrite them to invented fresh
backups or invent backup creation times. Unknown evidence must remain explicit.

## Publication, activation and rollback prerequisites

Selected shared workflows remain pinned to
`9307f9b80141f4ed9c463af360a1b13a49ca3353`. Pushes/PRs/Story merges validate only;
publication and deployment remain separate manual, main-only approved actions.
No workflow trigger or fleet governance migration is part of OP-500.

The local Linux candidate build is currently blocked by insufficient Docker
storage. No unrelated images, volumes, containers or caches were pruned. This
does not invalidate passing package/unit checks; candidate-image validation must
pass before activation. Production deployment/health evidence is unavailable.

After implementation review approval, integrate the necessary infrastructure
configuration into main and the provider Story into release/v0.6.0 only after
exact-head hosted checks pass. All R1 acceptance gaps then need an explicit
release/activation plan; do not infer permission to merge the release into main,
publish or deploy. OP-500 requires runtime acceptance, so it must stay open while
those actions are unauthorized. Resolve that lifecycle gate with the operator
explicitly rather than weakening AC6–8 or publishing to close the Story.

For approved activation, record known-good image/configuration/workspace and
schedule/sensor states; stop cadence before rollback; restore only the prior
approved configuration/image through the existing infrastructure rollback path;
retain journals and archives, verify health and avoid a competing daemon. Database
or destructive production recovery requires its own approval and is not a rollback
side effect. See [operations](operationalization.md) for the detailed contract.
