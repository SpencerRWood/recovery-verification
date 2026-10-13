"""Quiet-by-default standardized broker events; no direct channel delivery."""

import json
import os
from collections.abc import Callable
from http.client import HTTPException
from time import sleep
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import AwareDatetime

from recovery_verification.contract import ContractModel
from recovery_verification.scorecard import (
    Scorecard,
    TargetScore,
    ensure_no_runtime_secrets,
)


def _signature(target: TargetScore) -> tuple[object, ...]:
    return (
        target.owning_repository,
        target.readiness,
        target.full_recovery_verified,
        target.strongest_current_level,
        tuple((t.tier, t.status, t.reasons) for t in target.tiers),
        target.reasons,
    )


def notification_events(
    current: Scorecard, previous: Scorecard | None = None
) -> tuple[dict[str, object], ...]:
    """Required failures alert once per attempt; material changes include recovery."""
    prior = {t.target_id: t for t in previous.targets} if previous else {}
    if previous and previous.observed_at > current.observed_at:
        raise ValueError("notification_baseline_in_future")
    events: list[dict[str, object]] = []
    for target in current.targets:
        old = prior.get(target.target_id)
        failed = [t for t in target.tiers if t.status == "failed"]
        fresh_failures = [
            t
            for t in failed
            if old is None
            or not any(
                p.tier == t.tier
                and p.status == "failed"
                and p.latest_attempt == t.latest_attempt
                for p in old.tiers
            )
        ]
        changed = old is not None and _signature(old) != _signature(target)
        if not fresh_failures and not changed:
            continue
        tier = (
            fresh_failures[-1]
            if fresh_failures
            else next(
                (
                    t
                    for t, p in zip(
                        target.tiers, old.tiers if old else (), strict=False
                    )
                    if (t.status, t.reasons) != (p.status, p.reasons)
                ),
                target.tiers[-1],
            )
        )
        # Bind idempotency to the material state and evidence, not observation time.
        identity = uuid5(
            NAMESPACE_URL,
            "recovery-verification:"
            + json.dumps(
                [
                    target.target_id,
                    _signature(target),
                    target.owning_repository,
                    [
                        (
                            t.tier,
                            t.latest_attempt.evidence_reference
                            if t.latest_attempt
                            else None,
                        )
                        for t in fresh_failures
                    ],
                    [
                        _signature(old),
                        previous.history_reference,
                        current.observed_at.isoformat(),
                    ]
                    if changed and not fresh_failures and old and previous
                    else None,
                    tier.latest_attempt.evidence_reference
                    if tier.latest_attempt
                    else None,
                ],
                sort_keys=True,
            ),
        )
        events.append(
            {
                "schema_version": "1",
                "event_id": str(identity),
                "event_type": "recovery.verification-failed"
                if fresh_failures
                else "recovery.readiness-changed",
                "source": "recovery-verification",
                "severity": "error"
                if target.readiness == "NOT_READY"
                else "success"
                if target.readiness == "READY"
                else "warning",
                "occurred_at": (
                    tier.latest_attempt.evidence_timestamp
                    if fresh_failures and tier.latest_attempt
                    else current.observed_at
                ).isoformat(),
                "correlation_id": str(identity),
                "causation_id": None,
                "idempotency_key": "recovery-" + str(identity),
                "metadata": {},
                "data": {
                    "target": target.target_id,
                    "consumer": target.owning_repository,
                    "verification_tier": tier.tier,
                    "normalized_status": target.readiness,
                    "failed_checks": [
                        c.check_id
                        for c in tier.latest_attempt.diagnostics
                        if c.state == "failed"
                    ]
                    if tier.latest_attempt
                    else [],
                    "reasons": list(target.reasons),
                    "next_actions": list(target.next_actions),
                    "last_successful_relevant_verification": (
                        tier.last_success.timestamp.isoformat()
                        if tier.last_success
                        else None
                    ),
                    "tested_commit": tier.latest_attempt.tested_revision
                    if tier.latest_attempt
                    else target.tested_revision,
                    "evidence_reference": tier.latest_attempt.evidence_reference
                    if tier.latest_attempt
                    else None,
                    "failed_verifications": [
                        {
                            "verification_tier": failed.tier,
                            "reasons": list(failed.reasons),
                            "failed_checks": [
                                c.check_id
                                for c in failed.latest_attempt.diagnostics
                                if c.state == "failed"
                            ],
                            "tested_commit": failed.latest_attempt.tested_revision,
                            "evidence_reference": (
                                failed.latest_attempt.evidence_reference
                            ),
                            "last_successful_relevant_verification": (
                                failed.last_success.timestamp.isoformat()
                            )
                            if failed.last_success
                            else None,
                        }
                        for failed in fresh_failures
                        if failed.latest_attempt
                    ],
                },
            }
        )
    for event in events:
        data = cast("dict[str, Any]", event["data"])
        if len(json.dumps(data)) > 3500:
            data["details_omitted"] = True
            for key in ("failed_checks", "reasons", "next_actions"):
                data[key] = [str(item)[:128] for item in data[key][:1]]
            for failure in data["failed_verifications"]:
                failure["reasons"] = [
                    str(item)[:128] for item in failure["reasons"][:1]
                ]
                failure["failed_checks"] = [
                    str(item)[:128] for item in failure["failed_checks"][:1]
                ]
    ensure_no_runtime_secrets(json.dumps(events, sort_keys=True))
    return tuple(events)


class EventReceipt(ContractModel):
    schema_version: str = "1"
    record_id: UUID
    correlation_id: UUID
    accepted_at: AwareDatetime
    duplicate: bool


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        return None


def send_event(event: dict[str, object]) -> EventReceipt | None:  # noqa: PLR0911, PLR0912 -- Explicit destination, receipt and retry failure boundaries.
    """Runtime-injected source-bound token, bounded HTTPS, no proxies/redirects."""
    url = os.environ.get("RECOVERY_EVENTS_URL", "")
    token = os.environ.get("RECOVERY_EVENTS_TOKEN", "")
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError:
        return None
    if (
        not token
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or (
            parsed.scheme != "https"
            and not (
                parsed.scheme == "http"
                and (
                    parsed.hostname in {"127.0.0.1", "localhost", "::1"}
                    or (parsed.hostname == "wood-broker" and port == 8000)
                )
            )
        )
    ):
        return None
    payload = json.dumps(event, sort_keys=True).encode()
    try:
        ensure_no_runtime_secrets(payload.decode())
    except ValueError:
        return None
    if token in payload.decode():
        return None
    request = Request(  # noqa: S310 -- Validated HTTPS or loopback HTTP URL.
        url.rstrip("/") + "/v1/events",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer " + token,
        },
        method="POST",
    )
    for attempt in range(3):
        try:
            with build_opener(ProxyHandler({}), _NoRedirect()).open(
                request, timeout=5
            ) as response:
                raw = response.read(65537)
                if response.status == 201:
                    if len(raw) > 65536:
                        return None
                    receipt = EventReceipt.model_validate_json(raw)
                    if str(receipt.record_id) == event.get("event_id") and str(
                        receipt.correlation_id
                    ) == event.get("correlation_id"):
                        return receipt
                    return None
                if response.status not in {429, 500, 502, 503, 504}:
                    return None
        except HTTPError as error:
            if error.code not in {429, 500, 502, 503, 504}:
                return None
        except ValueError:
            return None
        except OSError, URLError, HTTPException:
            pass
        if attempt < 2:
            sleep(0.1 * (attempt + 1))
    return None


def publish_events(
    events: tuple[dict[str, object], ...],
    sender: Callable[[dict[str, object]], EventReceipt | bool | None] = send_event,
) -> bool:
    results = [sender(event) for event in events]
    return all(results)
