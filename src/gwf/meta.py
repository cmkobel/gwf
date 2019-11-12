import json
import os.path
import time
import logging

import lmdb
from flufl.lock import Lock

from .exceptions import GWFError


logger = logging.getLogger(__name__)


def open_db(path=os.path.join(".gwf", "meta.db")):
    return lmdb.open(path, lock=False)


class StateError(GWFError):
    pass


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


class TargetMeta:
    def __init__(
        self,
        target,
        submitted_at=None,
        started_at=None,
        ended_at=None,
        state=State.INITIAL_STATE,
    ):
        self.target = target
        self.submitted_at = submitted_at
        self.started_at = started_at
        self.ended_at = ended_at
        self.state = state

    def runtime(self):
        """Return runtime of the target in seconds.

        If the target is in state SUBMITTED, this method will return None.
        If the target is in state RUNNING, the elapsed time so far will be
        returned. Otherwise, the actual time that the target ran will be
        returned.
        """
        if self.state in (State.UNKNOWN, State.SUBMITTED):
            return None
        if self.state == State.RUNNING:
            return time.time() - self.started_at
        return self.ended_at - self.started_at

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
        dct = {
            "submitted_at": self.submitted_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "state": self.state,
        }
        payload = json.dumps(dct).encode("utf-8")
        lock = Lock(os.path.join(".gwf", "lock"))
        with lock, open_db() as db, db.begin(write=True) as txn:
            txn.put(self.target.name.encode("utf-8"), payload)

    @classmethod
    def from_payload(cls, target, payload=None):
        if payload is None:
            return cls(target)
        return cls(target, **json.loads(payload.decode("utf-8")))

    @classmethod
    def from_targets(cls, targets):
        lock = Lock(os.path.join(".gwf", "lock"))
        metas = []
        with lock, open_db() as db, db.begin() as txn:
            for target in targets:
                payload = txn.get(target.name.encode("utf-8"))
                meta = cls.from_payload(target, payload)
                metas.append(meta)
        return metas


def get_target_meta(target):
    return TargetMeta.from_targets([target])[0]


def get_target_metas(targets):
    return TargetMeta.from_targets(targets)
