# Consumer onboarding (OP-495)

Infrastructure and homelab implement the same v1 wire interface without importing
the provider. Each owns a version-controlled `recovery/consumer.v1.json` recipe
and `scripts/recovery.py` exporter/preflight. Export pins the exact clean source
revision rather than storing a self-referential hash in its own Git commit.

Both targets advertise readiness only. Bootstrap produces a normal Ansible
reconstruction plan; it never applies to a live target. Recovery directly declares
the existing previous-known-good `scripts/rollback.py` interface, with the owning
environment and preview default. Its required failed-release input and operational
serialization remain the consumer's contract. No compatibility forwarding wrappers,
duplicate rollback logic, or consumer-name branches were added to this provider.

## Source-grounded dependency references

`repo://path#section` means the authoritative source in the owning checkout at its
declared revision. This keeps image selection, service inventories, required
Infisical key names and endpoint routes in consumer Ansible/Compose files.
`git://owner/repository`, `path:///`, `mount:///` and `infisical://` declare repository,
storage/mount and secret scopes. They are references, not values or proof of access.
The manifests declare retained migration backup inputs with a conservative one-day
freshness limit. Those historical archives are not a current successful backup
attestation. Current backup policy, unpinned image tags and external availability
remain explicit readiness work; no fallback archive or guessed release is selected.

## Explicit generic invocation

Inspect the consumer repository, revision and readiness command before granting
execution trust. Export a manifest from its clean checkout, then invoke:

```sh
recovery-verification /tmp/consumer.v1.json --target TARGET \
  --checkout /path/to/trusted/consumer --invoke-check consumer_preflight
```

`--invoke-check` is an explicit trust grant for that revision's declared readiness
command. A manifest does not grant trust. `invoke_readiness(..., trusted=True)` is
the corresponding Python API. The adapter requires matching origin/revision and a
clean checkout, rejects symlinked/escaping/non-executable entrypoints, and rechecks
revision/cleanliness afterward. No bootstrap, recovery, weekly or drill command
is invoked by this adapter.

Execution uses argv without a shell or inherited application credentials. It
limits stdout to 64 KiB, discards stderr, enforces the declared timeout, terminates
the process group on completion/failure and never retries. Output is parsed into
strict `ConsumerResult/ConsumerCheck` models with bounded counts, unique IDs,
identifier-only reasons and conservative normalized states. Duplicate JSON keys,
extra fields, inconsistent states or an exit code inconsistent with the result
fail closed. Raw output and exception values never reach provider diagnostics.

The CLI returns target, owner, tested revision, check ID and normalized result.
Exit codes are 0 passed, 1 failed, 2 invalid invocation, 3 skipped/not applicable,
4 unavailable. Contract-only preflight behavior remains unchanged when no
invocation is requested.

## Coverage and audit disposition

Fixtures in `tests/fixtures` are consumer wire snapshots using synthetic revisions;
they are not live inventory or delivered-revision evidence. Tests prove both parse
through the same model and plan through the same adapter, reject stronger levels,
and preserve the provider/consumer boundary. Subprocess tests exercise explicit
trust, source identity, containment, process timeout, oversized/malformed output,
credential exclusion and false-success handling.

For a source-level integration check of the actual consumer implementations:

```sh
uv run python scripts/verify-consumers.py /path/to/infrastructure /path/to/homelab
```

This checked-in diagnostic copies Git source inputs, including uncommitted changes,
to temporary clean Git test snapshots; ignored runtime files and development
environments are not copied. It reuses each consumer's development tool PATH,
exports its actual manifest and invokes its actual preflight through the public
generic adapter. Temporary commits are test fixtures, never delivery commits.
It requires all local checks to pass and overall readiness to remain unavailable.
The normalized output reports source/snapshot revisions and includes the explicit
external-prerequisite limitation. Temporary resources are removed afterward.

Audit: both consumers already own canonical Ansible, rollback and health paths.
The missing onboarding surface was a versioned manifest and safe structured
preflight. Host apply/check-mode isolation, actual restore testing, backup policy
and live prerequisite probing are not established by existing syntax or rollback
checks. Consumer readiness intentionally remains unavailable until those
capabilities are independently proved. See consumer onboarding runbooks for the
owning files and operational boundaries.
