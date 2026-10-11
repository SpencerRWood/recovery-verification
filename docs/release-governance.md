# OP-499 release and deployment gates

## Approved release association

OpenProject project 3, planning Release 20 (`R1`), does not specify SemVer.
The user-approved provider association is **v0.6.0**, integration branch
`release/v0.6.0`, based on stable main `aa4318d6a6daba2f6d6db94f05f68bd4d0a728d6`.
Main and the latest published tag v0.5.1 are the same revision. OP-499 adds a
feature; the configured conventional-commit policy uses minor increments for
features before 1.0. The mapping is approved; no version file was changed.
Semantic-release owns the actual published version.

The authoritative workbook confirms all seven R1 Stories, including OP-500, in
[docs/releases/v0.6.0.md](releases/v0.6.0.md). OP-499 integration does not defer
OP-500 or approve release readiness, production activation or publication. The
existing Story branch/worktree/claim is retained under the explicit approved
continuation exception; its original base equals the integration base. No rebase,
reset or claim replacement is needed. Repository metadata declares the exact
release target. Wood 0.20.1 was installed locally from CI-verified foundation
source `a1ee806691274cc882f57940f3c97349d81c38c7`; release-target evidence is
supported without publishing tooling or merging unrelated migrations.

## Authorization matrix

| Event | Validation | Publication | Deployment |
| --- | --- | --- | --- |
| Feature push or PR | Yes | No | No |
| Story merge into provider release branch | Yes | No | No |
| Consumer metadata/governance merge into main | Yes | No | No |
| Provider release PR or merge into main | Yes | No | No |
| Manual main dispatch, approval false | No release execution | No | No |
| Manual main publication dispatch, `release_approved=true` | Required before publishing | Yes, after separate release approval | No |
| Manual consumer main deployment dispatch, `deployment_approved=true` | Existing target validation | No new release | Existing immutable release tag, after separate deployment approval |
| Approved manual rollback | Existing target validation | No new release | Existing prior release tag; automatic rollback on apply/health failure remains |
| Approved dispatch on feature/release/tag ref | No privileged operation | No | No |

Dispatch approval inputs record an operator's separate authorization; they do not
grant permission by themselves. Publication and deployment have separate inputs
and separate workflows. Neither a release result nor a Story merge invokes
consumer deployment. Manual deployments retain release resolution, protected
self-hosted runners/environments, validation, health checks, durable metadata and
automatic rollback. No recovery implementation or runtime configuration changes.
The separate recovery rollback workflow retains `apply=false` planning and requires
a manual main dispatch for `apply=true`; automatic invocations cannot apply it.

## Shared contracts and activation

All three shared publisher contracts and Ansible deployment use the same
`authorize-operation.yml` dependency. OP-499 callers always pass
`require_approval=true` and their operation-specific approval. The guard executes
before validation/publication or self-hosted deployment. Unauthorized requests
fail closed. Optional inputs default to the previous behavior for other callers;
this is an opt-in adoption, not a fleet migration. Older major refs are unchanged.

Shared helpers use an explicit `workflow_revision` rather than the unsupported
`job.workflow_sha` expression. Release validation uses the same shared source;
OP-499 callers bind the workflow and helper revision together. This removes the
existing expression error from the selected publication/validation paths.

Shared controls [PR #37](https://github.com/SpencerRWood/workflows/pull/37) passed
hosted CI and merged. Selected callers and helper revision inputs pin its verified
immutable merge SHA `9307f9b80141f4ed9c463af360a1b13a49ca3353`. Caller changes
must pass local and hosted checks before downstream merge recommendations.
No shared major tag, GitHub Release, image or deployment was published.

Proposed PR order: shared workflows → infrastructure and homelab callers/metadata
(both target main) → provider Story (targets the approved release integration
branch). Shared main merges have contract CI only; consumer main merges validate
only after the gated callers are pinned. Provider integration validates only.
Release integration into main and all publication/deployment require later,
separate release-readiness approval. Existing foundation PRs are not merged by
this task; the exact approved policy exception and verified tooling source are
retained in the R1 scope/evidence instead of migrating other repositories.
