"""Explicit local history persistence with bounded reads and atomic replacement."""

import fcntl
import json
import os
import tempfile
from pathlib import Path

from recovery_verification.contract import _reject_duplicate_keys
from recovery_verification.scorecard import (
    History,
    canonical,
    merge_history,
    parse_history,
    reference,
)

MAX_HISTORY_BYTES = 10 * 1024 * 1024


def read_bounded(path: Path) -> str:
    with path.open("rb") as stream:
        raw = stream.read(MAX_HISTORY_BYTES + 1)
    if len(raw) > MAX_HISTORY_BYTES:
        raise ValueError("evidence_history_too_large")
    return raw.decode("utf-8")


def read_history(path: Path) -> History:
    raw = read_bounded(path)
    try:
        document = json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
        if set(document) != {"history", "history_reference"}:
            raise ValueError("archive_contract")
        history = parse_history(json.dumps(document["history"]))
        if document["history_reference"] != reference(history):
            raise ValueError("archive_digest")
        return history
    except ValueError, TypeError:
        raise ValueError("invalid_evidence_archive") from None


def write_history(path: Path, history: History) -> None:
    """Lock and merge prior records; interrupted writes cannot replace the archive."""
    history = parse_history(canonical(history))
    descriptor = os.open(
        path.with_suffix(path.suffix + ".lock"), os.O_CREAT | os.O_RDWR, 0o600
    )
    with os.fdopen(descriptor, "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if path.exists():
            history = merge_history(read_history(path), history.entries)
        _write_archive(path, history)


def _write_archive(path: Path, history: History) -> None:
    raw = (
        json.dumps(
            {
                "history": history.model_dump(mode="json"),
                "history_reference": reference(history),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    if len(raw.encode()) > MAX_HISTORY_BYTES:
        raise ValueError("evidence_history_too_large")
    descriptor, temporary = tempfile.mkstemp(
        prefix=".recovery-history-", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        Path(temporary).replace(path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)
