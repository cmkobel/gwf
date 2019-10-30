import time
from pathlib import Path

import pytest

import gwf.conf


@pytest.fixture(autouse=True)
def no_version_check(request, monkeypatch):
    monkeypatch.setitem(gwf.conf.CONFIG_DEFAULTS, "check_updates", False)


@pytest.fixture
def no_sleep(request, monkeypatch):
    def sleep(seconds):
        pass

    monkeypatch.setattr(time, "sleep", sleep)


@pytest.fixture(autouse=True)
def isolated_working_dir(request, tmpdir):
    with tmpdir.as_cwd():
        Path(".gwf").mkdir()
        yield
