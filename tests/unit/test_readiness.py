"""Daily semantics: confirmed failure, uncertain services and historical evidence."""

import json
import os
import subprocess
from datetime import datetime, timedelta
from email.message import Message
from pathlib import Path
from typing import Any
from unittest.mock import Mock
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

import pytest

from recovery_verification import probes, readiness
from recovery_verification.cli import main
from recovery_verification.contract import Manifest, parse_manifest
from recovery_verification.invocation import InvocationError
from recovery_verification.models import ConsumerResult
from recovery_verification.probes import BoundedProbe, probe, result
from recovery_verification.readiness import assess_currency, run_readiness


@pytest.fixture
def manifest(document: dict[str, Any]) -> Manifest:
    return parse_manifest(json.dumps(document))


@pytest.fixture
def local(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        readiness, "_checkout", lambda *_: result("passed", "exact_checkout")
    )
    monkeypatch.setattr(
        readiness, "_interfaces", lambda *_: result("passed", "interfaces_present")
    )
    monkeypatch.setattr(
        readiness, "_consumer", lambda *_: result("passed", "syntax_valid")
    )


@pytest.mark.usefixtures("local")
@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ("passed", "READY"),
        ("failed", "NOT_READY"),
        ("unavailable", "UNKNOWN"),
        ("skipped", "UNKNOWN"),
        ("not_applicable", "READY"),
    ],
)
def test_required_check_states(
    manifest: Manifest, tmp_path: Path, state: str, expected: str
) -> None:
    report = run_readiness(
        manifest,
        {manifest.targets[0].id: tmp_path},
        lambda *_: result(state, "probe_result"),
    )
    assert report.readiness == expected
    assert all(
        item.timestamp.tzinfo and item.duration_seconds >= 0 for item in report.checks
    )
    assert all(
        item.tested_revision == manifest.targets[0].revision for item in report.checks
    )
    assert report == readiness.ReadinessReport.model_validate_json(
        report.model_dump_json()
    )


@pytest.mark.usefixtures("local")
def test_diagnostic_subset_never_claims_ready(
    manifest: Manifest, tmp_path: Path
) -> None:
    report = run_readiness(
        manifest,
        {manifest.targets[0].id: tmp_path},
        lambda *_: result("passed", "ok"),
        check_ids=("manifest_contract",),
    )
    assert report.readiness == "UNKNOWN"
    assert any(item.state == "skipped" for item in report.checks)


@pytest.mark.parametrize(
    ("targets", "checks"),
    [
        (("absent",), ()),
        (("example-linux", "example-linux"), ()),
        ((), ("absent",)),
        ((), ("checkout", "checkout")),
    ],
)
def test_bad_selection(
    manifest: Manifest, targets: tuple[str, ...], checks: tuple[str, ...]
) -> None:
    with pytest.raises(ValueError, match="selection"):
        run_readiness(
            manifest,
            {},
            lambda *_: result("passed", "ok"),
            target_ids=targets,
            check_ids=checks,
        )


def test_missing_checkout_prevents_probes(manifest: Manifest) -> None:
    runner = Mock(side_effect=AssertionError("unsafe execution"))
    assert run_readiness(manifest, {}, runner).readiness == "UNKNOWN"
    runner.assert_not_called()


@pytest.mark.usefixtures("local")
def test_invalid_selection_does_not_invoke_checks(
    manifest: Manifest, tmp_path: Path
) -> None:
    runner = Mock(side_effect=AssertionError("unexpected execution"))
    with pytest.raises(ValueError, match="selection"):
        run_readiness(
            manifest, {manifest.targets[0].id: tmp_path}, runner, check_ids=("absent",)
        )
    runner.assert_not_called()


@pytest.mark.usefixtures("local")
def test_source_drift_supersedes_failed_observations(
    manifest: Manifest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        readiness,
        "_checkout",
        Mock(
            side_effect=[
                result("passed", "exact_checkout"),
                result("unavailable", "checkout_changed"),
            ]
        ),
    )
    report = run_readiness(
        manifest,
        {manifest.targets[0].id: tmp_path},
        lambda *_: result("failed", "missing_prerequisite"),
    )
    assert report.currency == "superseded"
    assert report.readiness == "UNKNOWN"


def test_readiness_capability_required(document: dict[str, Any]) -> None:
    target = document["targets"][0]
    target["allowed_verification_levels"] = ["verification"]
    target["validation_commands"][0]["levels"] = ["verification"]
    target["cadence_overrides"] = []
    with pytest.raises(ValueError, match="capability"):
        run_readiness(parse_manifest(json.dumps(document)), {}, BoundedProbe())


@pytest.mark.usefixtures("local")
def test_stale_superseded_and_future_evidence(
    manifest: Manifest, tmp_path: Path
) -> None:
    report = run_readiness(
        manifest, {manifest.targets[0].id: tmp_path}, lambda *_: result("passed", "ok")
    )
    assert (
        assess_currency(
            report, manifest, now=report.timestamp, max_age_seconds=86400
        ).readiness
        == "READY"
    )
    stale = assess_currency(
        report,
        manifest,
        now=report.timestamp + timedelta(days=2),
        max_age_seconds=86400,
    )
    assert stale.currency == "stale"
    assert stale.readiness == "UNKNOWN"
    future = assess_currency(
        report,
        manifest,
        now=report.timestamp - timedelta(seconds=1),
        max_age_seconds=86400,
    )
    assert future.currency == "stale"
    changed = manifest.model_copy(
        update={
            "targets": (manifest.targets[0].model_copy(update={"revision": "2" * 40}),)
        }
    )
    superseded = assess_currency(
        report, changed, now=report.timestamp, max_age_seconds=86400
    )
    assert superseded.currency == "superseded"
    assert superseded.readiness == "UNKNOWN"
    with pytest.raises(ValueError, match="freshness"):
        assess_currency(report, manifest, now=datetime(2026, 1, 1), max_age_seconds=0)  # noqa: DTZ001 -- Reject naive timestamps.
    updates: tuple[dict[str, Any], ...] = (
        {"readiness": "NOT_READY"},
        {"checks": report.checks * 2},
        {"timestamp": report.timestamp.replace(tzinfo=None)},
        {"target_revisions": {}},
    )
    for update in updates:
        with pytest.raises(ValueError, match=r"readiness|identity|revision"):
            readiness.ReadinessReport.model_validate(report.model_dump() | update)


@pytest.mark.parametrize("state", ["failed", "unavailable", "skipped", "passed"])
def test_consumer_outcomes_are_preserved(
    manifest: Manifest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    wire = {
        "schema_version": "1",
        "mode": "preflight",
        "readiness_state": state,
        "checks": [{"id": "syntax", "state": state, "reason": "ansible_result"}],
    }
    monkeypatch.setattr(
        readiness,
        "invoke_readiness",
        lambda *_args, **_kwargs: ConsumerResult.model_validate_json(json.dumps(wire)),
    )
    assert readiness._consumer(manifest.targets[0], tmp_path, "validate").state == state
    monkeypatch.setattr(
        readiness, "invoke_readiness", Mock(side_effect=InvocationError("raw-secret"))
    )
    assert (
        readiness._consumer(manifest.targets[0], tmp_path, "validate").reason
        == "consumer_unavailable"
    )


@pytest.mark.parametrize("problem", ["none", "revision", "owner", "error"])
def test_checkout_identity(
    manifest: Manifest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, problem: str
) -> None:
    target = manifest.targets[0]

    def git(_root: Path, *args: str) -> str:
        if problem == "error":
            raise InvocationError("raw-secret")
        if args == ("rev-parse", "--show-toplevel"):
            return str(tmp_path.resolve())
        if args == ("rev-parse", "HEAD"):
            return "2" * 40 if problem == "revision" else target.revision
        if args[0] == "remote":
            return (
                "bad-owner"
                if problem == "owner"
                else f"https://github.com/{target.owning_repository}.git"
            )
        return ""

    monkeypatch.setattr(readiness, "_git", git)
    assert readiness._checkout(target, tmp_path).state == (
        "passed" if problem == "none" else "unavailable"
    )


def test_entrypoints_presence(manifest: Manifest, tmp_path: Path) -> None:
    target = manifest.targets[0]
    assert readiness._interfaces(target, tmp_path).state == "failed"
    for command in (
        target.entrypoints.bootstrap,
        target.entrypoints.recovery,
        *(item.command for item in target.validation_commands),
    ):
        file = tmp_path / command.entrypoint
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text("#!/bin/sh\nexit 0\n")
        file.chmod(0o755)
    assert readiness._interfaces(target, tmp_path).state == "passed"


def test_backup_fresh_stale_and_storage_namespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    file = tmp_path / "backup.dump"
    file.write_bytes(b"fixture")
    reference = "path://" + str(file)
    assert probe("backup", reference, tmp_path, 86400).state == "unavailable"
    monkeypatch.setenv("RECOVERY_LOCAL_STORAGE", "1")
    assert probe("backup", reference, tmp_path, 86400).state == "passed"
    os.utime(file, (1, 1))
    assert probe("backup", reference, tmp_path, 86400).reason == "backup_stale"
    assert (
        probe("storage", "path:///missing-fixture-path", tmp_path, None).state
        == "failed"
    )
    assert (
        probe("storage", "mount://" + str(tmp_path), tmp_path, None).reason
        == "required_mount_absent"
    )
    assert (
        probe("backup", "path://" + str(tmp_path), tmp_path, 86400).reason
        == "backup_artifact_unproven"
    )
    assert (
        probe("storage", "https://example.test", tmp_path, None).state == "unavailable"
    )
    monkeypatch.setattr(
        Path, "stat", Mock(side_effect=OSError("unreachable NAS secret"))
    )
    assert probe("storage", reference, tmp_path, None).reason == "storage_unreachable"


@pytest.mark.parametrize(
    ("kind", "reference", "state"),
    [
        ("tool", "missing-unique-tool-fixture", "unavailable"),
        ("tool", "git", "passed"),
        ("runbook", "repo://missing.md", "failed"),
        ("repository", "https://example.test", "unavailable"),
        ("secret", "unsupported://x", "unavailable"),
        ("artifact", "unsupported://x", "unavailable"),
        ("other", "unknown://x", "unavailable"),
    ],
)
def test_unsupported_and_missing_prerequisites(
    tmp_path: Path, kind: str, reference: str, state: str
) -> None:
    assert probe(kind, reference, tmp_path, None).state == state


def test_repository_availability_and_runbook(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for code, state in [(0, "passed"), (128, "unavailable")]:
        monkeypatch.setattr(
            subprocess,
            "run",
            Mock(return_value=subprocess.CompletedProcess([], code)),
        )
        assert (
            probe("repository", "git://example/repository", tmp_path, None).state
            == state
        )
    (tmp_path / "guide.md").write_text("runbook")
    assert probe("runbook", "repo://guide.md#section", tmp_path, None).state == "passed"
    with pytest.raises(ValueError, match="unsafe path"):
        probes.repository_path(tmp_path, "repo://../outside")


def test_secret_metadata_present_missing_and_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RECOVERY_INFISICAL_URL", "https://vault.example.test")
    monkeypatch.setenv("RECOVERY_INFISICAL_TOKEN", "never-log-token")
    reference = "infisical://" + "1" * 8 + "-1111-1111-1111-111111111111/dev/app/KEY"

    def metadata(url: str, *, token: str = "", head: bool = False) -> bytes:
        query = parse_qs(urlsplit(url).query)
        assert query["viewSecretValue"] == ["false"]
        assert query["expandSecretReferences"] == ["false"]
        assert token == "never-log-token"  # noqa: S105 -- Synthetic credential exclusion fixture.
        assert not head
        return b'{"secrets":[{"secretKey":"KEY","secretValue":""}]}'

    monkeypatch.setattr(probes, "request", metadata)
    observation = probe("secret", reference, tmp_path, None)
    assert observation.state == "passed"
    assert "never-log" not in observation.model_dump_json()
    monkeypatch.setattr(probes, "request", lambda *_args, **_kwargs: b'{"secrets":[]}')
    assert (
        probe("secret", reference, tmp_path, None).reason == "secret_reference_missing"
    )
    monkeypatch.setattr(
        probes,
        "request",
        lambda *_args, **_kwargs: (
            b'{"secrets":[],"imports":[{"secrets":[{"secretKey":"KEY"}]}]}'
        ),
    )
    assert probe("secret", reference, tmp_path, None).state == "passed"
    monkeypatch.setattr(
        probes,
        "request",
        Mock(
            side_effect=HTTPError(
                "https://vault.example.test", 503, "raw-secret", Message(), None
            )
        ),
    )
    assert probe("secret", reference, tmp_path, None).state == "unavailable"
    monkeypatch.setattr(
        probes,
        "request",
        lambda *_args, **_kwargs: (
            b'{"secrets":[{"secretKey":"KEY","secretValue":"never-emit"}]}'
        ),
    )
    assert (
        probe("secret", reference, tmp_path, None).reason
        == "secret_metadata_contract_failed"
    )
    monkeypatch.delenv("RECOVERY_INFISICAL_TOKEN")
    assert (
        probe("secret", reference, tmp_path, None).reason == "secret_probe_unconfigured"
    )
    assert (
        probe("secret", "infisical://Named/dev/app/KEY", tmp_path, None).reason
        == "secret_project_unconfigured"
    )
    assert (
        probe("secret", "infisical://Named/dev", tmp_path, None).reason
        == "secret_reference_unresolved"
    )


def test_secret_catalog(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "catalog.yml").write_text(
        "services:\n  app:\n    environment: dev\n"
        "    path: /app\n    required_keys: [KEY]\n"
    )
    assert (
        probe(
            "secret", "infisical://Named/dev#catalog.yml:services", tmp_path, None
        ).state
        == "unavailable"
    )
    assert (
        probe("secret", "infisical://Named/dev#catalog.yml", tmp_path, None).reason
        == "secret_catalog_unresolved"
    )
    monkeypatch.setenv(
        "RECOVERY_INFISICAL_PROJECTS",
        json.dumps({"Named": "11111111-1111-1111-1111-111111111111"}),
    )
    monkeypatch.setenv("RECOVERY_INFISICAL_TOKEN", "never-log")
    monkeypatch.setenv("RECOVERY_INFISICAL_URL", "https://vault.example.test")
    monkeypatch.setattr(
        probes,
        "request",
        lambda *_args, **_kwargs: b'{"secrets":[{"secretKey":"KEY"}]}',
    )
    assert (
        probe(
            "secret", "infisical://Named/dev#catalog.yml:services", tmp_path, None
        ).state
        == "passed"
    )


@pytest.mark.parametrize(
    "reference",
    [
        "oci://ubuntu:latest",
        "oci://owner/image@sha256:abc",
        "oci://ghcr.io/owner/image:1",
        "oci://busybox",
    ],
)
def test_artifact_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reference: str
) -> None:
    monkeypatch.setattr(probes, "request", lambda *_args, **_kwargs: b"")
    assert probe("artifact", reference, tmp_path, None).state == "passed"


def test_artifact_catalog_and_templates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(probes, "request", lambda *_args, **_kwargs: b"")
    (tmp_path / "compose.yml").write_text(
        "services:\n  app:\n    image: ubuntu:24.04\n"
    )
    assert probe("artifact", "repo://compose.yml", tmp_path, None).state == "passed"
    assert probe("artifact", "repo://missing", tmp_path, None).state == "failed"
    assert probe("artifact", "oci://app:${TAG}", tmp_path, None).state == "unavailable"
    (tmp_path / "empty.yml").write_text("services: {}\n")
    assert probe("artifact", "repo://empty.yml", tmp_path, None).state == "unavailable"
    assert probes.images([{"image": "ubuntu"}]) == ["ubuntu"]


@pytest.mark.parametrize(
    ("code", "expected"),
    [(404, "failed"), (503, "unavailable"), (403, "unavailable"), (401, "unavailable")],
)
def test_registry_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: int, expected: str
) -> None:
    monkeypatch.setattr(
        probes,
        "request",
        Mock(
            side_effect=HTTPError(
                "https://ghcr.io", code, "secret-body", Message(), None
            )
        ),
    )
    assert (
        probe("artifact", "oci://ghcr.io/example/app:1", tmp_path, None).state
        == expected
    )


def test_anonymous_registry_challenge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    headers = Message()
    headers["WWW-Authenticate"] = (
        'Bearer realm="https://auth.docker.io/token",service="registry.docker.io"'
    )
    challenge = HTTPError(
        "https://registry-1.docker.io",
        401,
        "auth",
        headers,
        None,
    )
    request_mock = Mock(
        side_effect=[challenge, b'{"token":"ephemeral-anonymous"}', b""]
    )
    monkeypatch.setattr(probes, "request", request_mock)
    assert probe("artifact", "oci://ubuntu:24.04", tmp_path, None).state == "passed"
    assert request_mock.call_args.kwargs["token"] == "ephemeral-anonymous"  # noqa: S105 -- Synthetic anonymous registry token.
    for response, expected in [(b"{}", "unavailable"), (b'{"token":"x"}', "failed")]:
        request_mock.side_effect = [
            challenge,
            response,
            HTTPError("https://registry-1.docker.io", 404, "missing", Message(), None),
        ]
        assert probe("artifact", "oci://ubuntu:24.04", tmp_path, None).state == expected


def test_worker_real_process_and_sanitized_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RECOVERY_LOCAL_STORAGE", "1")
    (tmp_path / "backup").write_bytes(b"fixture")
    assert (
        BoundedProbe()(
            "backup", "path://" + str(tmp_path / "backup"), tmp_path, 86400
        ).state
        == "passed"
    )
    monkeypatch.setenv("RECOVERY_INFISICAL_PROJECTS", "invalid-secret-payload")
    observation = BoundedProbe()(
        "secret", "infisical://Named/dev/app/KEY", tmp_path, None
    )
    assert observation.state == "unavailable"
    assert "payload" not in observation.model_dump_json()
    monkeypatch.setattr(
        probes, "_output", Mock(side_effect=InvocationError("raw-secret"))
    )
    assert BoundedProbe()("tool", "git", tmp_path, None).state == "unavailable"


@pytest.mark.usefixtures("local")
def test_daily_cli(
    manifest: Manifest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    file = tmp_path / "manifest.json"
    file.write_text(manifest.model_dump_json())
    monkeypatch.setattr(BoundedProbe, "__call__", lambda *_: result("passed", "ok"))
    assert (
        main(
            [
                str(file),
                "--daily",
                "--target",
                manifest.targets[0].id,
                "--checkout",
                str(tmp_path),
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["readiness"] == "READY"
    assert main([str(file), "--daily"]) == 2
    assert main([str(file), "--check", "checkout"]) == 2


def test_https_and_no_redirect() -> None:
    with pytest.raises(ValueError, match="endpoint"):
        probes.request("http://credential@example.test")
    probes.NoRedirect().redirect_request()
