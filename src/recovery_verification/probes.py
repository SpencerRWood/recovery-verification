"""Read-only prerequisite probes isolated behind a bounded worker process."""

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from time import time
from urllib.error import HTTPError
from urllib.parse import quote, unquote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

import yaml

from recovery_verification.invocation import _output, _stop
from recovery_verification.models import normalize_readiness
from recovery_verification.readiness import Observation

LIMIT = 1048576


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


def request(url: str, *, token: str = "", head: bool = False) -> bytes:
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
        "application/vnd.docker.distribution.manifest.v2+json"
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    opener = build_opener(ProxyHandler({}), NoRedirect())
    with opener.open(
        Request(url, headers=headers, method="HEAD" if head else "GET"),  # noqa: S310 -- HTTPS scheme validated above.
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


def artifact(reference: str, root: Path) -> Observation:  # noqa: PLR0911,PLR0912 -- Registry/capability failures have distinct outcomes.
    if reference.startswith("repo://"):
        path = repository_path(root, reference)
        if not path.exists():
            return result("failed", "artifact_catalog_missing")
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
    if registry == "registry-1.docker.io" and "/" not in repository:
        repository = "library/" + repository
    if "@" in repository:
        repository, tag = repository.rsplit("@", 1)
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
        if authority.hostname not in {
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
        response = json.loads(request(auth_url))
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
    token = os.environ.get("RECOVERY_INFISICAL_TOKEN", "")
    base = os.environ.get("RECOVERY_INFISICAL_URL", "")
    if not token or not base:
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
            "includeImports": "true",
            "recursive": "false",
            "includePersonalOverrides": "false",
        }
    )
    try:
        document = json.loads(
            request(base.rstrip("/") + "/api/v4/secrets?" + query, token=token)
        )
    except HTTPError:
        # Authentication/authorization, missing environment and service failures
        # do not establish that an individual required key is absent.
        return result("unavailable", "secret_service_unavailable")
    entries = list(document["secrets"])
    for imported in document.get("imports", []):
        entries.extend(imported["secrets"])
    if any(entry.get("secretValue") not in {None, ""} for entry in entries):
        return result("unavailable", "secret_metadata_contract_failed")
    return result(
        "passed"
        if any(entry.get("secretKey") == name for entry in entries)
        else "failed",
        "secret_reference_present"
        if any(entry.get("secretKey") == name for entry in entries)
        else "secret_reference_missing",
    )


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
        environment = {
            key: value
            for key, value in os.environ.items()
            if key
            in {
                "PATH",
                "RECOVERY_INFISICAL_URL",
                "RECOVERY_INFISICAL_TOKEN",
                "RECOVERY_INFISICAL_PROJECTS",
                "RECOVERY_LOCAL_STORAGE",
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
                output = _output(process, 30)
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
