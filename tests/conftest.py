import json
from pathlib import Path

import pytest


@pytest.fixture
def snapshot():
    return json.loads((Path(__file__).parent / "fixtures/matches.json").read_text())
