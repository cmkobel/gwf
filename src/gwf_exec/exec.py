import logging
import os
import os.path
import signal
import subprocess
import time
import sys

from gwf.exceptions import GWFError

logger = logging.getLogger(__name__)


class ExecutionError(GWFError):
    pass


class KilledError(ExecutionError):
    pass


class CancelledError(ExecutionError):
    pass


class Executor:
    def __init__(self, target, state):
        self.target = target
        self.state = state

        self._killed = False
        self._cancelled = False

        signal.signal(signal.SIGTERM, self._handle_kill)
        signal.signal(signal.SIGUSR1, self._handle_cancel)

    def _handle_kill(self, signum, frame):
        self._killed = True

    def _handle_cancel(self, signum, frame):
        self._cancelled = True

    def execute(self):
        try:
            self.state.running()

            process = subprocess.Popen(
                args=["/bin/bash", "-o", "pipefail", "-e"],
                shell=False,
                cwd=self.target.working_dir,
                env=os.environ,
                stdin=subprocess.PIPE,
                bufsize=0,
            )
            process.stdin.write(self.target.spec.encode("utf-8"))
            process.stdin.close()

            while not self._killed and not self._cancelled and process.poll() is None:
                time.sleep(0.1)

            if self._killed:
                logger.debug("process was killed")
                raise KilledError("process was killed")
            elif self._cancelled:
                logger.debug("process was cancelled")
                raise CancelledError("process was cancelled")
            elif process.returncode == 0:
                logger.debug("process completed")
                self.state.completed()
            else:
                logger.debug("process failed with return code %d", process.returncode)
                self.state.failed()

            return process.returncode
        except (KeyboardInterrupt, KilledError):
            process.kill()
            self.state.killed()
        except CancelledError:
            process.kill()
            self.state.cancelled()
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
