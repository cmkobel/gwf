import json
import os.path
import time
import logging

import attr
import lmdb

from .exceptions import GWFError


logger = logging.getLogger(__name__)

METADATA_FILE = os.path.join(".gwf", "meta.db")

STATE_DB_NAME = "state"

STATE_SHOULDRUN = "shouldrun"
STATE_SUBMITTED = "submitted"
STATE_RUNNING = "running"
STATE_COMPLETED = "completed"
STATE_FAILED = "failed"
STATE_CANCELLED = "cancelled"
STATE_KILLED = "killed"

ALL_STATES = (
    STATE_SHOULDRUN,
    STATE_SUBMITTED,
    STATE_RUNNING,
    STATE_COMPLETED,
    STATE_FAILED,
    STATE_CANCELLED,
    STATE_KILLED,
)

INITIAL_STATE = STATE_SHOULDRUN
END_STATES = (STATE_COMPLETED, STATE_FAILED, STATE_KILLED, STATE_CANCELLED)

TRANSITION_MAP = {
    STATE_SHOULDRUN: [STATE_SUBMITTED],
    STATE_SUBMITTED: [STATE_RUNNING],
    STATE_RUNNING: END_STATES,
}


def open_db():
    return lmdb.open(METADATA_FILE, max_dbs=2)


class StateError(GWFError):
    pass


@attr.s
class TargetState:
    _CACHE = {}

    db = attr.ib(repr=False)
    target = attr.ib()
    submitted_at = attr.ib(type=float, default=None)
    started_at = attr.ib(type=float, default=None)
    ended_at = attr.ib(type=float, default=None)
    state = attr.ib(type=str, default=INITIAL_STATE)

    def walltime(self):
        """Return walltime of the target in seconds.

        This method will return *None* if the target is not in an end state,
        that is, completed, failed, or killed.
        """
        if self.state not in END_STATES:
            return None
        return self.ended_at - self.started_at

    def runtime(self):
        """Return runtime of the target in seconds so far.

        This method will return *None* if the target is not in the running
        state.
        """
        if self.state != STATE_RUNNING:
            return None
        return time.time() - self.started_at

    def move_to(self, state, autocommit=True):
        if self.state not in TRANSITION_MAP:
            raise StateError("Cannot move out of state {}".format(self.state))
        if state not in TRANSITION_MAP[self.state]:
            raise StateError(
                "Cannot move from state {} to state {}".format(self.state, state)
            )

        current_time = time.time()
        if state == STATE_SUBMITTED:
            self.submitted_at = current_time
        elif state == STATE_RUNNING:
            self.started_at = current_time
        elif state in END_STATES:
            self.ended_at = current_time
        self.state = state

        if autocommit:
            self.commit()

    def __getattr__(self, name):
        if name.startswith("is_"):
            suffix = name[3:]
            if suffix not in ALL_STATES:
                raise AttributeError(name)
            return lambda: self.state == suffix

        if name not in ALL_STATES:
            raise AttributeError(name)
        return lambda: self.move_to(name)

    def reset(self, autocommit=True):
        self.submitted_at = None
        self.started_at = None
        self.ended_at = None
        self.state = INITIAL_STATE
        if autocommit:
            self.commit()

    def commit(self):
        with self.db.begin(write=True) as txn:
            dct = attr.asdict(
                self,
                filter=attr.filters.exclude(
                    attr.fields(TargetState).db, attr.fields(TargetState).target
                ),
            )
            payload = json.dumps(dct).encode("utf-8")
            txn.put(self.target.name.encode("utf-8"), payload)

    @classmethod
    def from_payload(cls, db, target, payload=None):
        if target not in TargetState._CACHE:
            logger.debug("Fetching target state from database")
            if payload is None:
                return cls(db, target)
            state = cls(db, target, **json.loads(payload))
            TargetState._CACHE[target] = state
        return TargetState._CACHE[target]

    @classmethod
    def from_target(cls, db, target):
        with db.begin() as txn:
            payload = txn.get(target.name.encode("utf-8"))
            return cls.from_payload(db, target, payload)
