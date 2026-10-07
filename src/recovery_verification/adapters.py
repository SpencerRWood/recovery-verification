"""Generic declarative boundary for future consumer invocation adapters."""

from dataclasses import dataclass

from recovery_verification.contract import Command, Level, Target


@dataclass(frozen=True)
class Invocation:
    """Unexecuted plan: checkout/revision ownership remains explicit."""

    owning_repository: str
    revision: str
    command: Command


def validation_plan(target: Target, level: Level) -> tuple[Invocation, ...]:
    """Select declarations generically; never dispatch by consumer identity."""
    if level not in target.allowed_verification_levels:
        raise ValueError("unsupported verification level")
    return tuple(
        Invocation(target.owning_repository, target.revision, check.command)
        for check in target.validation_commands
        if level in check.levels
    )
