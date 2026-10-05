"""Helpers for testing Tracelet's public API without hiding failures."""

import importlib
import importlib.util
import sys
from pathlib import Path


_SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))


def public_symbol(name: str):
    spec = importlib.util.find_spec("tracelet")
    if spec is None:
        raise AssertionError("the public tracelet package is not implemented")
    package = importlib.import_module("tracelet")
    if not hasattr(package, name):
        raise AssertionError(f"tracelet must export {name}")
    return getattr(package, name)
