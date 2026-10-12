# Repository guidance

This application adopts release-branch integration for the explicitly approved
R1 scope in [docs/releases/v0.6.0.md](docs/releases/v0.6.0.md). OP-499's existing
branch, worktree and durable claim are retained under that recorded exception;
its PR must target release/v0.6.0. Do not reset or migrate the existing claim.

Follow the [canonical codex-config branching policy](https://github.com/SpencerRWood/codex-config/blob/docs/release-branch-methodology-20261011/docs/branching-policy.md)
and the approval recorded in
the R1 scope. Story integration, release readiness, publication and deployment
are separate authorization boundaries. No other repository adoption is implied.

Run the declared Wood validation and verification contracts. Provider logic stays
generic; consumer recovery implementations remain in their owning repositories.
Unsupported monthly bootstrap must never become full recovery assurance.
