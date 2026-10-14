"""Read-only prerequisite probes isolated behind a bounded worker process."""

import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from time import time
from typing import Literal, Self
from urllib.error import HTTPError
from urllib.parse import quote, unquote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

import yaml
from pydantic import Field, model_validator

from recovery_verification.contract import ContractModel, Reference
from recovery_verification.invocation import _output, _stop
from recovery_verification.models import normalize_readiness
from recovery_verification.readiness import Observation

LIMIT = 1048576
_metadata_login: tuple[str, str, str, str, float] | None = None


class ArtifactCatalog(ContractModel):
    schema_version: Literal["1"]
    references: tuple[Reference, ...] = Field(min_length=1, max_length=128)

    @model_validator(mode="after")
    def exact_artifacts(self) -> Self:
        if len(set(self.references)) != len(self.references) or any(
            not (
                re.fullmatch(r"oci://[a-zA-Z0-9_./:-]+@sha256:[0-9a-f]{64}", item)
                or item.startswith("build://")
                or item.startswith("image-archive:///")
            )
            for item in self.references
        ):
            raise ValueError("immutable_artifact_catalog_required")
        return self


def result(state: str, reason: str) -> Observation:
    return Observation.model_validate(
        {
            "state": state,
            "reason": reason,
            "category": "recovery_prerequisite"
            if state == "failed"
            else "verification_infrastructure"
            if state in {"unavailable", "skipped"}
            else "none",
        }
    )


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args: object, **kwargs: object) -> None:  # noqa: ARG002
        return None


def request(
    url: str,
    *,
    token: str = "",
    head: bool = False,
    basic: tuple[str, str] | None = None,
    payload: dict[str, str] | None = None,
) -> bytes:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.username
        or parsed.password
        or not parsed.hostname
    ):
        raise ValueError("invalid endpoint")
    headers = {
        "Accept": "application/json, application/vnd.oci.image.manifest.v1+json, "
        "application/vnd.oci.image.index.v1+json, "
        "application/vnd.docker.distribution.manifest.v2+json, "
        "application/vnd.docker.distribution.manifest.list.v2+json"
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if basic:
        if parsed.netloc != "ghcr.io" or parsed.path != "/token" or token:
            raise ValueError("unsupported_registry_credential_authority")
        credentials = base64.b64encode(":".join(basic).encode()).decode()
        headers["Authorization"] = "Basic " + credentials
    if payload is not None:
        if parsed.path != "/api/v1/auth/universal-auth/login" or basic or token:
            raise ValueError("unsupported_metadata_auth_endpoint")
        headers["Content-Type"] = "application/json"
    opener = build_opener(ProxyHandler({}), NoRedirect())
    with opener.open(
        Request(  # noqa: S310 -- HTTPS scheme validated above.
            url,
            headers=headers,
            data=json.dumps(payload).encode() if payload is not None else None,
            method="POST" if payload is not None else "HEAD" if head else "GET",
        ),
        timeout=5,
    ) as response:
        data: bytes = response.read(LIMIT + 1)
        if len(data) > LIMIT:
            raise ValueError("response limit")
        return data


def repository_path(root: Path, reference: str) -> Path:
    parsed = urlsplit(reference)
    name = parsed.netloc + parsed.path
    path = root / unquote(name)
    if not path.resolve().is_relative_to(root.resolve()) or any(
        part.is_symlink() for part in (path, *path.parents) if part != root
    ):
        raise ValueError("unsafe path")
    return path


def combine(items: list[Observation], reason: str) -> Observation:
    return result(normalize_readiness(tuple(item.state for item in items)), reason)


def images(document: object) -> list[str]:
    if isinstance(document, dict):
        found = [
            value
            for key, value in document.items()
            if (key == "image" or str(key).endswith("_image_ref"))
            and isinstance(value, str)
        ]
        return found + [image for value in document.values() for image in images(value)]
    if isinstance(document, list):
        return [image for value in document for image in images(value)]
    return []


def explicit_catalog(path: Path, root: Path) -> Observation:
    try:
        catalog = ArtifactCatalog.model_validate_json(path.read_text())
    except ValueError:
        return result("failed", "artifact_catalog_invalid")
    return combine(
        [artifact(item, root) for item in catalog.references],
        "artifact_catalog_checked",
    )


def image_archive(reference: str) -> Observation:  # noqa: PLR0911 -- Preserve distinct artifact/custody failures.
    """Verify an independently retained encrypted image and its retrieval proof."""
    if os.environ.get("RECOVERY_LOCAL_STORAGE") != "1":
        return result("unavailable", "storage_namespace_unconfigured")
    parsed = urlsplit(reference)
    path = Path(unquote(parsed.path))
    if parsed.netloc or parsed.query or parsed.fragment or ".." in path.parts:
        return result("failed", "image_archive_reference_invalid")
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > LIMIT:
            return result("failed", "image_archive_metadata_invalid")
        document = json.loads(path.read_text())
        name = document["artifact"]
        if (
            document["schema_version"] != "1"
            or document["format"] != "docker-save-age"
            or Path(name).name != name
            or name in {".", ".."}
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", document["image_id"])
            or not re.fullmatch(r"[0-9a-f]{64}", document["ciphertext_sha256"])
            or not re.fullmatch(r"[0-9a-f]{64}", document["archive_sha256"])
            or not document["recipient"].startswith("age1")
            or not re.fullmatch(
                r"[^/\s]+:/[^\s]+", document["independent_storage_source"]
            )
        ):
            return result("failed", "image_archive_metadata_invalid")
        filesystem = json.loads(
            subprocess.check_output(  # noqa: S603 -- Fixed read-only mount inspector.
                [  # noqa: S607 -- The deployed Linux runtime supplies findmnt.
                    "findmnt",
                    "--json",
                    "--target",
                    str(path),
                    "--output",
                    "SOURCE,FSTYPE",
                ],
                timeout=5,
                stderr=subprocess.DEVNULL,
            )
        )["filesystems"]
        if filesystem != [
            {"source": document["independent_storage_source"], "fstype": "nfs"}
        ]:
            return result("failed", "image_archive_storage_mismatch")
        artifact_path = path.parent / name
        if artifact_path.is_symlink() or not artifact_path.is_file():
            return result("failed", "image_archive_missing")
        if artifact_path.stat().st_size != document["size_bytes"]:
            return result("failed", "image_archive_integrity_failed")
        with artifact_path.open("rb") as stream:
            if stream.read(22) != b"age-encryption.org/v1\n":
                return result("failed", "image_archive_encryption_invalid")
            stream.seek(0)
            checksum = hashlib.file_digest(stream, "sha256").hexdigest()
        if checksum != document["ciphertext_sha256"]:
            return result("failed", "image_archive_integrity_failed")
        proof_path = path.with_name("retrieval.json")
        if not proof_path.is_file() or proof_path.is_symlink():
            return result("unavailable", "image_archive_retrieval_unverified")
        proof = json.loads(proof_path.read_text())
        if (
            proof.get("state") != "passed"
            or proof.get("plaintext_persisted") is not False
            or any(
                proof.get(key) != document[key]
                for key in ("image_id", "archive_sha256", "ciphertext_sha256")
            )
        ):
            return result("unavailable", "image_archive_retrieval_unverified")
    except OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError:
        return result("unavailable", "image_archive_unreadable")
    return result("passed", "image_archive_available")


def artifact(reference: str, root: Path) -> Observation:  # noqa: PLR0911,PLR0912 -- Registry/capability failures have distinct outcomes.
    if reference.startswith("image-archive://"):
        return image_archive(reference)
    if reference.startswith("build://"):
        source = repository_path(root, "repo://" + reference.removeprefix("build://"))
        return result(
            "unavailable" if source.is_file() else "failed",
            "artifact_publication_required"
            if source.is_file()
            else "artifact_build_source_missing",
        )
    if reference.startswith("repo://"):
        path = repository_path(root, reference)
        if not path.exists():
            return result("failed", "artifact_catalog_missing")
        if path.is_file() and path.suffix == ".json":
            return explicit_catalog(path, root)
        files = (
            [path]
            if path.is_file()
            else sorted((*path.rglob("*.yml"), *path.rglob("*.yaml")))
        )
        if len(files) > 128:
            return result("unavailable", "artifact_catalog_limit")
        declared = sorted(
            {
                image
                for file in files
                for image in images(yaml.safe_load(file.read_text()))
            }
        )
        if not declared:
            return result("unavailable", "artifact_catalog_unresolved")
        return combine(
            [artifact("oci://" + image, root) for image in declared],
            "artifact_catalog_checked",
        )
    if not reference.startswith("oci://"):
        return result("unavailable", "artifact_reference_unsupported")
    image = reference.removeprefix("oci://")
    if (
        not re.fullmatch(r"[a-zA-Z0-9_./:@-]+", image)
        or ".." in image
        or "://" in image
    ):
        return result("unavailable", "artifact_reference_unresolved")
    first, _, rest = image.partition("/")
    registry = (
        first if rest and ("." in first or ":" in first) else "registry-1.docker.io"
    )
    repository = rest if registry == first else image
    # LinuxServer documents LSCR as a vanity endpoint for its GHCR images.
    # Resolve the public catalog authority explicitly; never follow arbitrary
    # authentication redirects or forward credentials to a challenge host.
    if registry == "lscr.io" and repository.startswith("linuxserver/"):
        registry = "ghcr.io"
    if registry == "registry-1.docker.io" and "/" not in repository:
        repository = "library/" + repository
    if "@" in repository:
        repository, tag = repository.rsplit("@", 1)
        # A tag is only a human-readable hint when a digest is selected. It is
        # not part of the registry repository name or anonymous pull scope.
        repository = repository.rsplit(":", 1)[0]
    elif ":" in repository:
        repository, tag = repository.rsplit(":", 1)
    else:
        tag = "latest"
    url = f"https://{registry}/v2/{repository}/manifests/{tag}"
    try:
        request(url, head=True)
    except HTTPError as error:
        if error.code == 404:
            return result("failed", "artifact_missing")
        challenge = error.headers.get("WWW-Authenticate", "")
        if error.code != 401 or not challenge.startswith("Bearer "):
            return result("unavailable", "registry_unavailable")
        fields = dict(re.findall(r'(realm|service|scope)="([^"\r\n]+)"', challenge))
        realm = fields.get("realm", "")
        authority = urlsplit(realm)
        if authority.netloc not in {
            registry,
            "auth.docker.io" if registry == "registry-1.docker.io" else registry,
        }:
            return result("unavailable", "registry_auth_unsupported")
        auth_url = (
            realm
            + "?"
            + urlencode(
                {
                    "service": fields.get("service", registry),
                    "scope": f"repository:{repository}:pull",
                }
            )
        )
        username = os.environ.get("RECOVERY_GHCR_USERNAME", "")
        credential = os.environ.get("RECOVERY_GHCR_TOKEN", "")
        try:
            response = json.loads(
                request(auth_url, basic=(username, credential))
                if registry == "ghcr.io" and username and credential
                else request(auth_url)
            )
        except HTTPError:
            return result("unavailable", "registry_auth_unavailable")
        token = response.get("token", response.get("access_token"))
        if not isinstance(token, str):
            return result("unavailable", "registry_auth_unavailable")
        try:
            request(url, token=token, head=True)
        except HTTPError as retry:
            return result(
                "failed" if retry.code == 404 else "unavailable",
                "artifact_missing" if retry.code == 404 else "registry_unavailable",
            )
    return result("passed", "artifact_available")


def metadata_token(base: str, client: str, credential: str) -> str:
    """Mint bounded metadata identity tokens in memory; no operator-token fallback."""
    global _metadata_login  # noqa: PLW0603 -- Worker-local cache dies with the bounded probe process.
    if (
        _metadata_login is not None
        and _metadata_login[:3] == (base, client, credential)
        and time() < _metadata_login[4]
    ):
        return _metadata_login[3]
    document = json.loads(
        request(
            base.rstrip("/") + "/api/v1/auth/universal-auth/login",
            payload={"clientId": client, "clientSecret": credential},
        )
    )
    token, lifetime = document.get("accessToken"), document.get("expiresIn")
    if (
        not isinstance(token, str)
        or not token
        or isinstance(lifetime, bool)
        or not isinstance(lifetime, int)
        or not 10 <= lifetime <= 900
    ):
        raise ValueError("metadata_identity_token_contract_invalid")
    _metadata_login = (base, client, credential, token, time() + lifetime - 5)
    return token


def secrets(reference: str, root: Path) -> Observation:  # noqa: PLR0911 -- Explicit metadata-only capability guards.
    parsed = urlsplit(reference)
    if parsed.scheme != "infisical":
        return result("unavailable", "secret_reference_unsupported")
    parts = parsed.path.strip("/").split("/")
    project = unquote(parsed.netloc)
    if parsed.fragment:
        source, separator, key = parsed.fragment.partition(":")
        if not separator:
            return result("unavailable", "secret_catalog_unresolved")
        catalog = yaml.safe_load(repository_path(root, "repo://" + source).read_text())
        services = catalog[key]
        required = [
            f"infisical://{quote(project, safe='')}/{service['environment']}"
            f"{service['path']}/{name}"
            for service in services.values()
            for name in service["required_keys"]
        ]
        return combine(
            [secrets(item, root) for item in required], "secret_catalog_checked"
        )
    if len(parts) < 2:
        return result("unavailable", "secret_reference_unresolved")
    project_ids = json.loads(os.environ.get("RECOVERY_INFISICAL_PROJECTS", "{}"))
    project_id = project_ids.get(project, project)
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", project_id):
        return result("unavailable", "secret_project_unconfigured")
    client = os.environ.get("RECOVERY_INFISICAL_CLIENT_ID", "")
    credential = os.environ.get("RECOVERY_INFISICAL_CLIENT_SECRET", "")
    base = os.environ.get("RECOVERY_INFISICAL_URL", "")
    if not client or not credential or not base:
        return result("unavailable", "secret_probe_unconfigured")
    environment, name = parts[0], parts[-1]
    path = "/" + "/".join(parts[1:-1])
    query = urlencode(
        {
            "projectId": project_id,
            "environment": environment,
            "secretPath": path,
            "viewSecretValue": "false",
            "expandSecretReferences": "false",
            "includeImports": "false",
            "type": "shared",
        }
    )
    try:
        token = metadata_token(base, client, credential)
        document = json.loads(
            request(
                base.rstrip("/")
                + "/api/v4/secrets/"
                + quote(name, safe="")
                + "?"
                + query,
                token=token,
            )
        )
    except HTTPError, ValueError:
        # Authentication/authorization, missing environment and service failures
        # do not establish that an individual required key is absent.
        return result("unavailable", "secret_service_unavailable")
    entry = document.get("secret")
    if (
        not isinstance(entry, dict)
        or document.get("imports")
        or entry.get("secretValue") not in (None, "")
        or entry.get("secretKey") != name
    ):
        return result("unavailable", "secret_metadata_contract_failed")
    return result("passed", "secret_reference_present")


def probe(kind: str, reference: str, root: Path, max_age: int | None) -> Observation:  # noqa: PLR0911,PLR0912 -- Declarative kind dispatch and storage guards.
    if kind == "secret":
        return secrets(reference, root)
    if kind == "artifact":
        return artifact(reference, root)
    if kind == "repository":
        owner = reference.removeprefix("git://")
        if not reference.startswith("git://") or not re.fullmatch(
            r"[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+", owner
        ):
            return result("unavailable", "repository_reference_unsupported")
        status = subprocess.run(  # noqa: S603 -- Validated owner; argv has no shell.
            [  # noqa: S607 -- Operator tool PATH.
                "git",
                "ls-remote",
                "--exit-code",
                f"https://github.com/{owner}.git",
                "HEAD",
            ],
            env={
                "PATH": os.environ.get("PATH", os.defpath),
                "GIT_TERMINAL_PROMPT": "0",
            },
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
            check=False,
        )
        return result(
            "passed" if status.returncode == 0 else "unavailable",
            "repository_available"
            if status.returncode == 0
            else "repository_service_unavailable",
        )
    if kind == "tool":
        return result(
            "passed" if shutil.which(reference) else "unavailable",
            "tool_present" if shutil.which(reference) else "tool_unavailable",
        )
    if kind == "runbook" and reference.startswith("repo://"):
        return result(
            "passed" if repository_path(root, reference).exists() else "failed",
            "runbook_present"
            if repository_path(root, reference).exists()
            else "runbook_missing",
        )
    if kind in {"storage", "backup"}:
        if os.environ.get("RECOVERY_LOCAL_STORAGE") != "1":
            return result("unavailable", "storage_namespace_unconfigured")
        parsed = urlsplit(reference)
        if (
            parsed.scheme not in {"path", "mount"}
            or parsed.netloc
            or not parsed.path.startswith("/")
        ):
            return result("unavailable", "storage_reference_unsupported")
        path = Path(unquote(parsed.path))
        try:
            stat = path.stat()
        except FileNotFoundError:
            return result("failed", "storage_missing")
        except OSError:
            return result("failed", "storage_unreachable")
        if parsed.scheme == "mount" and not path.is_mount():
            return result("failed", "required_mount_absent")
        if not os.access(path, os.R_OK):
            return result("failed", "storage_unreadable")
        if kind == "backup":
            if not path.is_file() or stat.st_size == 0:
                return result("unavailable", "backup_artifact_unproven")
            if max_age is None or not 0 <= time() - stat.st_mtime <= max_age:
                return result("failed", "backup_stale")
        return result(
            "passed", "backup_fresh" if kind == "backup" else "storage_reachable"
        )
    return result("unavailable", "reference_unsupported")


class BoundedProbe:
    """Kill slow filesystem/network work; never expose exceptions or raw output."""

    def __call__(
        self, kind: str, reference: str, root: Path, max_age: int | None
    ) -> Observation:
        archive_catalog = False
        if kind == "artifact" and reference.startswith("repo://"):
            try:
                path = repository_path(root, reference)
                if path.is_file() and path.suffix == ".json":
                    catalog = ArtifactCatalog.model_validate_json(path.read_text())
                    archive_catalog = any(
                        item.startswith("image-archive://")
                        for item in catalog.references
                    )
            except OSError, ValueError:
                pass
        # Full ciphertext checksums can exceed the network-probe budget on NFS.
        # Bound them explicitly within the existing one-hour recovery run limit.
        timeout = (
            900 if archive_catalog or reference.startswith("image-archive://") else 30
        )
        environment = {
            key: value
            for key, value in os.environ.items()
            if key
            in {
                "PATH",
                "RECOVERY_INFISICAL_URL",
                "RECOVERY_INFISICAL_CLIENT_ID",
                "RECOVERY_INFISICAL_CLIENT_SECRET",
                "RECOVERY_INFISICAL_PROJECTS",
                "RECOVERY_LOCAL_STORAGE",
                "RECOVERY_GHCR_USERNAME",
                "RECOVERY_GHCR_TOKEN",
            }
        }
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        with subprocess.Popen(
            [sys.executable, "-m", "recovery_verification.probes"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=environment,
            start_new_session=True,
        ) as process:
            assert process.stdin is not None  # noqa: S101
            process.stdin.write(
                json.dumps(
                    {
                        "kind": kind,
                        "reference": reference,
                        "root": str(root),
                        "max_age": max_age,
                    }
                ).encode()
            )
            process.stdin.close()
            try:
                output = _output(process, timeout)
                observation = Observation.model_validate_json(output)
                return (
                    observation
                    if process.returncode == 0
                    else result("unavailable", "probe_failed")
                )
            except OSError, ValueError, subprocess.SubprocessError:
                return result("unavailable", "probe_unavailable")
            finally:
                _stop(process)


def main() -> None:
    try:
        document = json.loads(sys.stdin.buffer.read(65536))
        observation = probe(
            document["kind"],
            document["reference"],
            Path(document["root"]),
            document["max_age"],
        )
    except Exception:
        observation = result("unavailable", "probe_unavailable")
    sys.stdout.write(observation.model_dump_json() + "\n")


if __name__ == "__main__":
    main()
