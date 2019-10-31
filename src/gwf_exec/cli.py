import logging
import sys

from gwf.core import workflow_from_path
from gwf.models import get_target_state

from .exec import Executor

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def main(workflow_path, target_name):
    workflow = workflow_from_path(workflow_path)
    target = workflow.targets[target_name]

    state = get_target_state(target)
    executor = Executor(target, state)
    return executor.execute()


if __name__ == "__main__":
    returncode = main(workflow_path=sys.argv[1], target_name=sys.argv[2])
    sys.exit(returncode)
