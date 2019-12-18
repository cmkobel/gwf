from pathlib import Path

import pytest

from gwf.cli import main


SIMPLE_WORKFLOW = """from gwf import Workflow

gwf = Workflow()
gwf.target('Target1', inputs=[], outputs=['a.txt'])
gwf.target('Target2', inputs=['a.txt'], outputs=['b.txt'])
gwf.target('Target3', inputs=['a.txt'], outputs=['c.txt'])
"""


@pytest.fixture(autouse=True)
def simple_workflow():
    with open("workflow.py", "w") as fileobj:
        fileobj.write(SIMPLE_WORKFLOW)


@pytest.fixture(autouse=True)
def simple_workflow_files():
    Path("a.txt").touch()
    Path("b.txt").touch()
    Path("c.txt").touch()


def test_clean_output_from_non_endpoints(cli_runner):
    args = ["-b", "testing", "clean"]
    cli_runner.invoke(main, args, input="y\n")

    assert not Path("a.txt").exists()
    assert Path("b.txt").exists()
    assert Path("c.txt").exists()


def test_clean_output_from_all_targets(cli_runner):
    args = ["-b", "testing", "clean", "--all"]
    cli_runner.invoke(main, args, input="y\n")

    assert not Path("a.txt").exists()
    assert not Path("b.txt").exists()
    assert not Path("c.txt").exists()


def test_clean_output_from_single_endpoint_target(cli_runner):
    args = ["-b", "testing", "clean", "--all", "Target2"]
    cli_runner.invoke(main, args)

    assert Path("a.txt").exists()
    assert not Path("b.txt").exists()
    assert Path("c.txt").exists()


def test_clean_output_from_two_targets(cli_runner):
    args = ["-b", "testing", "clean", "--all", "Target1", "Target2"]
    cli_runner.invoke(main, args)

    assert not Path("a.txt").exists()
    assert not Path("b.txt").exists()
