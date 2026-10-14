"""Dedicated identity rotation and encrypted image evidence fail closed."""

import hashlib
import json
import subprocess
from email.message import Message
from pathlib import Path
from unittest.mock import Mock
from urllib.error import HTTPError

import pytest

from recovery_verification import probes


def test_metadata_identity_refreshes_without_persisting_or_using_operator_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(probes, "_metadata_login", None)
    clock = Mock(return_value=100.0)
    monkeypatch.setattr(probes, "time", clock)
    login = Mock(return_value=b'{"accessToken":"test-only","expiresIn":900}')
    monkeypatch.setattr(probes, "request", login)
    assert (
        probes.metadata_token("https://vault.example", "client", "fixture")
        == "test-only"
    )
    assert (
        probes.metadata_token("https://vault.example", "client", "fixture")
        == "test-only"
    )
    assert login.call_count == 1
    assert login.call_args.kwargs == {
        "payload": {"clientId": "client", "clientSecret": "fixture"}
    }
    clock.return_value = 1000.0
    probes.metadata_token("https://vault.example", "client", "fixture")
    assert login.call_count == 2
    probes.metadata_token("https://vault.example", "client", "rotated-fixture")
    assert login.call_count == 3


@pytest.mark.parametrize("lifetime", [True, 0, 9, 901, "900"])
def test_unbounded_or_invalid_identity_tokens_are_rejected(
    monkeypatch: pytest.MonkeyPatch, lifetime: object
) -> None:
    monkeypatch.setattr(probes, "_metadata_login", None)
    monkeypatch.setattr(
        probes,
        "request",
        lambda *_a, **_k: json.dumps(
            {"accessToken": "fixture", "expiresIn": lifetime}
        ).encode(),
    )
    with pytest.raises(ValueError, match="metadata_identity_token_contract_invalid"):
        probes.metadata_token("https://vault.example", "client", "fixture")


def test_image_archive_requires_ciphertext_integrity_and_bound_retrieval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RECOVERY_LOCAL_STORAGE", "1")
    monkeypatch.setattr(
        subprocess,
        "check_output",
        lambda *_a, **_k: (
            b'{"filesystems":[{"source":"nas:/recovery","fstype":"nfs"}]}'
        ),
    )
    ciphertext = b"age-encryption.org/v1\nfixture"
    (tmp_path / "image.tar.age").write_bytes(ciphertext)
    metadata = {
        "schema_version": "1",
        "format": "docker-save-age",
        "image_id": "sha256:" + "a" * 64,
        "artifact": "image.tar.age",
        "recipient": "age1fixture",
        "archive_sha256": "b" * 64,
        "ciphertext_sha256": hashlib.sha256(ciphertext).hexdigest(),
        "size_bytes": len(ciphertext),
        "independent_storage_source": "nas:/recovery",
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(metadata))
    reference = "image-archive://" + str(path)
    assert (
        probes.artifact(reference, tmp_path).reason
        == "image_archive_retrieval_unverified"
    )
    proof = {
        "state": "passed",
        "plaintext_persisted": False,
        **{k: metadata[k] for k in ("image_id", "archive_sha256", "ciphertext_sha256")},
    }
    (tmp_path / "retrieval.json").write_text(json.dumps(proof))
    assert probes.artifact(reference, tmp_path).state == "passed"
    proof["image_id"] = "sha256:" + "c" * 64
    (tmp_path / "retrieval.json").write_text(json.dumps(proof))
    assert probes.artifact(reference, tmp_path).state == "unavailable"
    (tmp_path / "image.tar.age").write_bytes(ciphertext[:-1] + b"x")
    assert (
        probes.artifact(reference, tmp_path).reason == "image_archive_integrity_failed"
    )


@pytest.mark.parametrize("status", [403, 404])
def test_denied_or_missing_scope_does_not_claim_a_missing_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    monkeypatch.setenv("RECOVERY_INFISICAL_URL", "https://vault.example")
    monkeypatch.setenv("RECOVERY_INFISICAL_CLIENT_ID", "fixture-client")
    monkeypatch.setenv("RECOVERY_INFISICAL_CLIENT_SECRET", "fixture-secret")
    monkeypatch.setattr(probes, "metadata_token", lambda *_: "fixture")
    monkeypatch.setattr(
        probes,
        "request",
        Mock(
            side_effect=HTTPError("https://vault.example", status, "", Message(), None)
        ),
    )
    reference = "infisical://11111111-1111-1111-1111-111111111111/dev/app/KEY"
    assert probes.secrets(reference, tmp_path).state == "unavailable"
