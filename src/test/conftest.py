"""Pytest bootstrap: make the shared test fixtures importable.

The tests import ``functional_test_support`` directly, so this directory has to
be on ``sys.path`` no matter which directory pytest is started from.
"""
import sys
from pathlib import Path

TEST_ROOT = Path(__file__).resolve().parent

if str(TEST_ROOT) not in sys.path:
    sys.path.insert(0, str(TEST_ROOT))
