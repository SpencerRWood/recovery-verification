"""Consumer-neutral wire fixtures, not live consumer onboarding."""

import json
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def document() -> dict[str, Any]:
    path = Path(__file__).parents[1] / "examples/consumer-manifest.v1.json"
    result: dict[str, Any] = json.loads(path.read_text())
    return result
