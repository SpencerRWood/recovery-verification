"""Exercise real committed dependency changes without executing recovery jobs."""

import json
import subprocess
from pathlib import Path

import pytest
from dagster import build_sensor_context

from recovery_verification.dagster.triggers import recovery_relevant_changes
from recovery_verification.orchestration import OrchestrationConfig, changed_paths


def git(root: Path, *args: str) -> str:
    return subprocess.run(  # noqa: S603 -- Disposable local Git fixture.
        ["git", *args],  # noqa: S607
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


@pytest.mark.parametrize(
    "case",
    [
        (
            "environments/dev.yml",
            "dagster_restore_backup_path: /old\n",
            "dagster_restore_backup_path: /new\n",
        ),
        (
            "compose/dagster/compose.yml",
            "services:\n  dagster:\n    volumes: [old:/backup:ro]\n",
            "services:\n  dagster:\n    volumes: [new:/backup:ro]\n",
        ),
        (
            "environments/dev.yml",
            "secret_reference: old/ref\n",
            "secret_reference: new/ref\n",
        ),
    ],
)
def test_committed_configuration_changes_trigger_readiness(
    *,
    case: tuple[str, str, str],
    document: dict[str, object],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, before, after = case
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.name", "Sensor fixture")
    git(tmp_path, "config", "user.email", "fixture@example.invalid")
    source = tmp_path / path
    source.parent.mkdir(parents=True)
    source.write_text(before)
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "test: baseline")
    old = git(tmp_path, "rev-parse", "HEAD")
    source.write_text(after)
    git(tmp_path, "commit", "-qam", "test: dependency change")
    new = git(tmp_path, "rev-parse", "HEAD")
    manifest = json.loads(json.dumps(document))
    manifest["targets"][0]["revision"] = new
    config = OrchestrationConfig(
        manifest_json=json.dumps(manifest),
        checkouts={"example-linux": str(tmp_path)},
        state_directory=str(tmp_path / "state"),
        environment="test",
    )
    monkeypatch.setattr(
        "recovery_verification.dagster.triggers.load_config", lambda: config
    )
    assert changed_paths(tmp_path, old, new) == (path,)
    context = build_sensor_context(cursor=json.dumps({"example-linux": old}))
    tick = recovery_relevant_changes.evaluate_tick(context)
    assert tick.run_requests
    assert len(tick.run_requests) == 1
    assert tick.run_requests[0].job_name == "recovery_readiness_daily"
    assert (
        tick.run_requests[0].run_key
        == f"change:test:example-linux:{old}:{new}:readiness"
    )
    assert tick.cursor == json.dumps({"example-linux": new}, sort_keys=True)
    # A rename away from a recovery path must retain the original path in the diff.
    source.rename(tmp_path / "README.md")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "test: rename dependency")
    renamed = git(tmp_path, "rev-parse", "HEAD")
    assert set(changed_paths(tmp_path, new, renamed)) == {path, "README.md"}
