import pytest

import gwf.conf


@pytest.fixture(autouse=True)
def no_version_check(request, monkeypatch):
    monkeypatch.setitem(gwf.conf.CONFIG_DEFAULTS, "check_updates", False)