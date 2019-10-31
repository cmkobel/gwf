import json
import os.path
import time
import logging

import attr
import lmdb

from .exceptions import GWFError


logger = logging.getLogger(__name__)

METADATA_FILE = os.path.join(".gwf", "meta.db")


class State:
    UNKNOWN = "unknown"
    SUBMITTED = "submitted"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    KILLED = "killed"

    ALL_STATES = (UNKNOWN, SUBMITTED, RUNNING, COMPLETED, FAILED, CANCELLED, KILLED)

    INITIAL_STATE = UNKNOWN
    END_STATES = (COMPLETED, FAILED, KILLED, CANCELLED)

    TRANSITION_MAP = {UNKNOWN: [SUBMITTED], SUBMITTED: [RUNNING], RUNNING: END_STATES}


def open_db():
    return lmdb.open(METADATA_FILE)


class StateError(GWFError):
    pass


@attr.s
class TargetMeta:
    _CACHE = {}

    db = attr.ib(repr=False)
    target = attr.ib()
    submitted_at = attr.ib(type=float, default=None)
    started_at = attr.ib(type=float, default=None)
    ended_at = attr.ib(type=float, default=None)
    state = attr.ib(type=str, default=State.INITIAL_STATE)

    def walltime(self):
        """Return walltime of the target in seconds.

        This method will return *None* if the target is not in an end state,
        that is, completed, failed, or killed.
        """
        if self.state not in State.END_STATES:
            return None
        return self.ended_at - self.started_at

    def runtime(self):
        """Return runtime of the target in seconds so far.

        This method will return *None* if the target is not in the running
        state.
        """
        if self.state != State.RUNNING:
            return None
        return time.time() - self.started_at

    def move_to(self, state, autocommit=True):
        if self.state not in State.TRANSITION_MAP:
            raise StateError("Cannot move out of state {}".format(self.state))
        if state not in State.TRANSITION_MAP[self.state]:
            raise StateError(
                "Cannot move from state {} to state {}".format(self.state, state)
            )

        current_time = time.time()
        if state == State.SUBMITTED:
            self.submitted_at = current_time
        elif state == State.RUNNING:
            self.started_at = current_time
        elif state in State.END_STATES:
            self.ended_at = current_time
        self.state = state

        if autocommit:
            self.commit()

    def __getattr__(self, name):
        if name.startswith("is_"):
            suffix = name[3:]
            if suffix not in State.ALL_STATES:
                raise AttributeError(name)
            return lambda: self.state == suffix

        if name not in State.ALL_STATES:
            raise AttributeError(name)

        def move_to_wrapper(*args, **kwargs):
            self.move_to(name, *args, **kwargs)

        return move_to_wrapper

    def reset(self, autocommit=True):
        self.submitted_at = None
        self.started_at = None
        self.ended_at = None
        self.state = State.INITIAL_STATE
        if autocommit:
            self.commit()

    def commit(self):
        with self.db.begin(write=True) as txn:
            dct = attr.asdict(
                self,
                filter=attr.filters.exclude(
                    attr.fields(TargetMeta).db, attr.fields(TargetMeta).target
                ),
            )
            payload = json.dumps(dct).encode("utf-8")
            txn.put(self.target.name.encode("utf-8"), payload)

    def __str__(self):
        return self.state

    @classmethod
    def from_payload(cls, db, target, payload=None):
        logger.debug("Fetching target metadata from database")
        if payload is None:
            return cls(db, target)
        state = cls(db, target, **json.loads(payload))
        return state

    @classmethod
    def from_target(cls, db, target):
        with db.begin() as txn:
            payload = txn.get(target.name.encode("utf-8"))
            return cls.from_payload(db, target, payload)


def get_target_meta(target, db=None):
    if db is None:
        db = open_db()
    return TargetMeta.from_target(db, target)
