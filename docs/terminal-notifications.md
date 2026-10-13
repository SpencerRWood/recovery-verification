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

Wood Events renders `recovery.run-completed` as one Backup & Recovery Scorecard
message and routes it through a source/event-specific Telegram policy. Recovery
Telegram sends commit a dispatch intent before contacting Telegram. A crash or
ambiguous response cannot automatically resend the same message; it is visible
as dispatching or delivery-uncertain and requires operator reconciliation.
Telegram supplies no send idempotency key, so guaranteed delivery and guaranteed
absence of duplicates cannot both be claimed after an uncertain remote result.
A known HTTP429 rejection can retry; ordinary unrelated notifications retain
their existing policy.

After exact artifact deployment and source credential/routing validation,
enable this notification sensor first through Dagster's supported controls,
run one approved non-destructive readiness job, inspect event and channel
receipts/content, and repeat the same accepted event to verify no additional
Telegram message. Then enable the existing schedules and change sensor and read
back states/next ticks. Do not run a destructive monthly drill for scheduling
tests. The recovery-specific queue tag limit must be deployed before activating
change/scheduled execution; the shared operation lock remains a final guard.
