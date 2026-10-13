"""Prevent loss of encrypted consumer runtime prerequisites."""

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_execution_extra_supplies_consumer_schema_runtime() -> None:
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert (
        "jsonschema==4.26.0" in config["project"]["optional-dependencies"]["execution"]
    )


def test_image_supplies_source_locales_utc_and_age() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()
    assert "locales-all tzdata age" in dockerfile
    assert "LANG=C.UTF-8 LC_ALL=C.UTF-8 TZ=UTC" in dockerfile
