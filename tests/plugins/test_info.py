import os
import json

import pytest

from gwf.cli import main


SIMPLE_WORKFLOW = """from gwf import Workflow

gwf = Workflow()
gwf.target('Target1', inputs=[], outputs=['a.txt'])
gwf.target('Target2', inputs=['a.txt'], outputs=['b.txt'])
gwf.target('Target3', inputs=['a.txt'], outputs=['c.txt'])
"""


@pytest.fixture
def simple_workflow():
    with open("workflow.py", "w") as fileobj:
        fileobj.write(SIMPLE_WORKFLOW)


def test_info_all_targets(cli_runner, simple_workflow):
    args = ["-b", "testing", "info"]
    result = cli_runner.invoke(main, args)
    doc = json.loads(result.output)

    assert "Target1" in doc
    assert "Target2" in doc
    assert "Target3" in doc


def test_info_single_target(cli_runner, simple_workflow):
    args = ["-b", "testing", "info", "Target1"]
    result = cli_runner.invoke(main, args)
    doc = json.loads(result.output)

    assert "Target1" in doc
    assert "Target2" not in doc
    assert "Target3" not in doc
