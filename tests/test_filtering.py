from unittest.mock import create_autospec

import pytest

from gwf import Target
from gwf.core import Graph, Scheduler, TargetStatus
from gwf.filtering import EndpointFilter, NameFilter, StatusFilter
<<<<<<< HEAD
from gwf.meta import get_target_meta
=======
from gwf.models import get_target_state
>>>>>>> Clean up and fix tests to use TargetState


@pytest.fixture
def backend(mocker):
    return mocker.Mock()


def test_filter_status_completed(backend):
    target = Target.empty("TestTarget")
    graph = Graph.from_targets({"TestTarget": target})

    scheduler = Scheduler(backend=backend, graph=graph)
    scheduler.should_run = create_autospec(scheduler.should_run, spec_set=True)

    status_filter = StatusFilter(scheduler=scheduler, status=[TargetStatus.COMPLETED])

<<<<<<< HEAD
    # state = get_target_meta(target)
=======
    # state = get_target_state(target)
>>>>>>> Clean up and fix tests to use TargetState

    # state.reset()

    status_filter.scheduler.should_run.return_value = False
    assert list(status_filter.apply([target])) == [target]

    status_filter.scheduler.should_run.return_value = True
    assert list(status_filter.apply([target])) == []


def test_filter_status_shouldrun(backend):
    target = Target.empty("TestTarget")
    graph = Graph.from_targets({"TestTarget": target})

    scheduler = Scheduler(backend=backend, graph=graph)
    scheduler.should_run = create_autospec(scheduler.should_run, spec_set=True)

<<<<<<< HEAD
    status_filter = StatusFilter(scheduler=scheduler, status=[TargetStatus.INCOMPLETE])

    state = get_target_meta(target)

    state.reset()

=======
    status_filter = StatusFilter(scheduler=scheduler, status=[TargetStatus.SHOULDRUN])

    state = get_target_state(target)

    state.reset()

>>>>>>> Clean up and fix tests to use TargetState
    status_filter.scheduler.should_run.return_value = False
    assert list(status_filter.apply([target])) == []

    status_filter.scheduler.should_run.return_value = True
    assert list(status_filter.apply([target])) == [target]

    state.submitted()

    status_filter.scheduler.should_run.return_value = False
    assert list(status_filter.apply([target])) == []

    status_filter.scheduler.should_run.return_value = True
    assert list(status_filter.apply([target])) == []

    state.running()

    status_filter.scheduler.should_run.return_value = False
    assert list(status_filter.apply([target])) == []

    status_filter.scheduler.should_run.return_value = True
    assert list(status_filter.apply([target])) == []


def test_filter_status_running(backend):
    target = Target.empty("TestTarget")
    graph = Graph.from_targets({"TestTarget": target})

    scheduler = Scheduler(backend=backend, graph=graph)
    scheduler.should_run = create_autospec(scheduler.should_run, spec_set=True)

    status_filter = StatusFilter(scheduler=scheduler, status=[TargetStatus.RUNNING])
    status_filter.scheduler.should_run.return_value = True

<<<<<<< HEAD
    state = get_target_meta(target)
=======
    state = get_target_state(target)
>>>>>>> Clean up and fix tests to use TargetState

    state.reset()
    assert list(status_filter.apply([target])) == []

    state.submitted()
    assert list(status_filter.apply([target])) == []

    state.running()
    assert list(status_filter.apply([target])) == [target]


def test_filter_status_submitted(backend):
    target = Target.empty("TestTarget")
    graph = Graph.from_targets({"TestTarget": target})

    scheduler = Scheduler(backend=backend, graph=graph)
    scheduler.should_run = create_autospec(scheduler.should_run, spec_set=True)

    status_filter = StatusFilter(scheduler=scheduler, status=[TargetStatus.SUBMITTED])
    status_filter.scheduler.should_run.return_value = True

<<<<<<< HEAD
    state = get_target_meta(target)
=======
    state = get_target_state(target)
>>>>>>> Clean up and fix tests to use TargetState

    state.reset()
    assert list(status_filter.apply([target])) == []

    state.submitted()
    assert list(status_filter.apply([target])) == [target]

    state.running()
    assert list(status_filter.apply([target])) == []


def test_filter_name():
    target1 = Target.empty("Foo")
    target2 = Target.empty("Bar")
    target3 = Target.empty("FooBar")

    name_filter = NameFilter(patterns=["Foo"])
    assert set(name_filter.apply([target1, target2, target3])) == {target1}

    name_filter = NameFilter(patterns=["Foo*"])
    assert set(name_filter.apply([target1, target2, target3])) == {target1, target3}

    name_filter = NameFilter(patterns=["Foo", "Bar"])
    assert set(name_filter.apply([target1, target2, target3])) == {target1, target2}


def test_filter_endpoint():
    target1 = Target.empty("Foo")
    target2 = Target.empty("Bar")
    target3 = Target.empty("FooBar")

    endpoint_filter = EndpointFilter(endpoints={target1})
    assert set(endpoint_filter.apply([target1, target2, target3])) == {target1}

    endpoint_filter = EndpointFilter(endpoints={target1, target3})
    assert set(endpoint_filter.apply([target1, target2, target3])) == {target1, target3}

    endpoint_filter = EndpointFilter(endpoints={target1}, mode="exclude")
    assert set(endpoint_filter.apply([target1, target2, target3])) == {target2, target3}

    endpoint_filter = EndpointFilter(endpoints={target1, target3}, mode="exclude")
    assert set(endpoint_filter.apply([target1, target2, target3])) == {target2}
