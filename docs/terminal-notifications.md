# Terminal Backup & Recovery Scorecard notifications

`recovery_run_notifications` observes persisted SUCCESS, FAILURE and CANCELED
events independently of job resource teardown. It never starts a recovery job.
It is stopped by default. Dagster supports bounded terminal-status event reads;
the sensor stores one storage-ID cursor for each terminal status in the durable
`terminal-notifications.json` journal. The configured UTC start time determines
the approved backlog; changing it with an existing journal fails explicitly.
Only this code location's provider jobs (or tagged durable manual runs) qualify.

Set `notifications_enabled`, an explicit `notification_start_time`, the HTTPS
`notification_dagster_url`, and the exact `notification_code_location` in the
non-secret orchestration configuration. Runtime-injected RECOVERY_EVENTS_URL
and RECOVERY_EVENTS_TOKEN must authorize source `recovery-verification`.
The supported private endpoint `http://wood-broker:8000` is restricted to the
existing container network; arbitrary plain HTTP destinations are rejected.
Public endpoints require HTTPS. No proxy or redirect forwarding is allowed.

Each run UUID determines one stable event ID and idempotency key. The sensor
atomically retains the frozen consolidated event and advances its event cursor
before sending. A lost broker acknowledgment retries identical content; a
matched broker receipt is persisted before removing the pending event. Broker
acceptance does not establish Telegram delivery: inspect Wood Notify's channel
attempts and stored Telegram message ID separately. Limits are 100 pending
notices and 10,000 retained receipts; reaching a
limit stops processing explicitly and requires reviewed archival/maintenance.
Each tick scans at most five records per status and attempts at most two broker
deliveries; existing pending events are attempted before new ingestion so a
recovered broker can drain a full outbox. More backlog continues on later ticks.

Scorecards use retained evidence at or before the terminal-event timestamp,
with the run's manifest when available. Notifications show backup freshness,
integrity, each assurance tier, failures/prerequisites and a Dagster run evidence
link. Missing or unreadable evidence reports UNKNOWN/UNAVAILABLE. Representative
fixtures never establish real backup integrity; a checksum alone also does not
prove successful integrity verification. A successful execution can still
report NOT_READY. Runtime-smoke/preflight runs do not gain recovery assurance.
The attended operational CLI now uses the configured persistent Dagster instance
so its terminal result can be observed; DAGSTER_HOME must select that instance.

Execution teardown retains reports and scorecards but no longer emits automatic
per-target operational alerts. The explicit standalone scorecard CLI remains an
independent interface; do not invoke its `--notify` option as a second notifier
for Dagster runs. Historical execution journal notification fields are historical
records, not the authority for the new terminal outbox.

Wood Events renders `recovery.run-completed` as an editorial Backup & Recovery
report and routes it through the existing source/event-specific Telegram policy.
The initial summary distinguishes execution, backup prerequisite evidence,
integrity, freshness, tier results and recovery readiness. Backup availability
does not prove producer execution; absent producer outcome evidence is reported
as execution unverified. Weekly integrity must bind real tested artifact digests
to the latest backup provenance. Fixture evidence cannot attest to production
recovery. Tier evidence includes dates; a digest-verified execution journal is
required to call evidence current to this run. Missing run binding is labeled
historical, including for failed/canceled runs. The evidence link always points
to this terminal run. Execution summaries are sanitized classifications, never
exception traces.

`notification_presentation.py` owns diagnostic descriptions and actionable
findings. Failed, skipped and unavailable checks remain distinct; passed and
not-applicable catalog diagnostics are informational. Unknown diagnostics retain
safe English labels and exact references in event findings and linked evidence.
Findings merge only by diagnostic identity, with both environment labels retained;
the unsupported-bootstrap reason is one underlying capability issue even when
represented by both a tier status and a check. OP-543 is a separate outstanding
custody requirement, not a scorecard evaluation override.

Structured `report_blocks` are additive event data; existing event identities,
outbox receipts, cutoffs and replay behavior are unchanged. Wood Notify freezes
HTML/plain parts at acceptance. Reports fitting Telegram's 4096-character limit
are single messages; longer reports retain every finding, continue category
headings in each part, and place the single evidence link last. UTF-16 counting
also bounds the plain fallback. Only an individually oversized finding splits.
Synthetic formatting uses `terminal_event(..., synthetic=True)`: TEST heading,
explicit simulated evidence, informational severity and separate event identity.
It never writes scorecard history or emits incident-resolution events.

Recovery
Telegram sends commit a dispatch intent before contacting Telegram. A crash or
ambiguous response cannot automatically resend the same message; it is visible
as dispatching or delivery-uncertain and requires operator reconciliation.
Telegram supplies no send idempotency key, so guaranteed delivery and guaranteed
absence of duplicates cannot both be claimed after an uncertain remote result.
A known HTTP429 rejection can retry; ordinary unrelated notifications retain
their existing policy.

Multipart dispatch uses `notify_recovery_parts` (Events migration 0005): stable
part IDs and ordered positions under the original logical request, immutable
payloads, independent attempts and confirmed Telegram message IDs. Confirmed
parts never resend on retry/restart. A failed or uncertain part stops later
parts; the scoped notification detail endpoint exposes every part's state.
Known rejection retries are bounded per part. A definite HTML HTTP400 rejection
can fall back to escaped plain text; uncertain acceptance never triggers that
fallback. Pending/uncertain multipart state pins its retention evidence.
Reconcile dispatching/delivery-uncertain with Telegram before any reviewed state
repair; there is no automatic exactly-once claim. Ordinary safe retry resumes
only failed parts. Deploy Events compatibility/migration before the producer.

After exact artifact deployment and source credential/routing validation,
enable this notification sensor first through Dagster's supported controls,
run one approved non-destructive readiness job, inspect event and channel
receipts/content, and repeat the same accepted event to verify no additional
Telegram message. Then enable the existing schedules and change sensor and read
back states/next ticks. Do not run a destructive monthly drill for scheduling
tests. The recovery-specific queue tag limit must be deployed before activating
change/scheduled execution; the shared operation lock remains a final guard.
