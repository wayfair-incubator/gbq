import importlib
import importlib.metadata
import tomllib
from pathlib import Path

import pytest

import gbq


def _installed() -> bool:
    try:
        importlib.metadata.version("gbq")
    except importlib.metadata.PackageNotFoundError:
        return False
    return True


# CI installs the project; the devbox image only installs its dependencies
@pytest.mark.skipif(not _installed(), reason="gbq is not installed")
def test_version_matches_pyproject():
    pyproject = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text()
    )
    assert gbq.__version__ == pyproject["project"]["version"]


def test_version_when_not_installed(monkeypatch):
    def not_installed(name):
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", not_installed)
    try:
        assert importlib.reload(gbq).__version__ == "0.0.0+unknown"
    finally:
        monkeypatch.undo()
        importlib.reload(gbq)
