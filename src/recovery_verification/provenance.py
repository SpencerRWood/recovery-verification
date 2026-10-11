"""Allowlisted execution identifiers; absence is evidence, never inferred success."""

from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, Field, model_validator

from recovery_verification.contract import Command, ContractModel, Identifier


class BackupProvenance(ContractModel):
    source_id: Identifier
    kind: Literal["recovery_backup", "representative_fixture"] = "recovery_backup"
    artifact_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")] | None = None
    timestamp: AwareDatetime | None = None


class ExecutionProvenance(ContractModel):
    playbooks: tuple[str, ...] | None = None
    inventories: tuple[str, ...] | None = None
    inline_inventory: Literal["localhost"] | None = None
    backups: tuple[BackupProvenance, ...] = ()

    @model_validator(mode="after")
    def safe_identifiers(self) -> Self:
        for paths in (self.playbooks, self.inventories):
            if paths is not None:
                if not paths or len(set(paths)) != len(paths):
                    raise ValueError("invalid_provenance_identifiers")
                for path in paths:
                    Command(entrypoint=path, args=(), timeout_seconds=1)
        if len({b.source_id for b in self.backups}) != len(self.backups):
            raise ValueError("duplicate_backup_provenance")
        return self
