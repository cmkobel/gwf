from collections import Counter

import click

from ..backends import backend_from_config
from ..core import Scheduler, TargetStatus, graph_from_config
from ..meta import get_target_metas
from ..filtering import StatusFilter, EndpointFilter, NameFilter, filter_generic


TABLE_FORMAT = (
    "{name:<{name_col_width}}  {status:<32}  {duration:>9}  {percentage:>7.2%}"
)


STATUS_COLORS = {
    TargetStatus.INCOMPLETE: "magenta",
    TargetStatus.KILLED: "magenta",
    TargetStatus.CANCELLED: "magenta",
    TargetStatus.FAILED: "magenta",
    TargetStatus.SUBMITTED: "yellow",
    TargetStatus.RUNNING: "blue",
    TargetStatus.COMPLETED: "green",
}

STATUS_HUMAN = {
    TargetStatus.INCOMPLETE: "shouldrun (incomplete)",
    TargetStatus.KILLED: "shouldrun (killed)",
    TargetStatus.CANCELLED: "shouldrun (cancelled)",
    TargetStatus.FAILED: "shouldrun (failed)",
    TargetStatus.SUBMITTED: "submitted",
    TargetStatus.RUNNING: "  running",
    TargetStatus.COMPLETED: "completed",
}

STATUS_ORDER = (
    TargetStatus.INCOMPLETE,
    TargetStatus.SUBMITTED,
    TargetStatus.RUNNING,
    TargetStatus.COMPLETED,
    TargetStatus.KILLED,
    TargetStatus.CANCELLED,
    TargetStatus.FAILED,
)


class StatusDistribution(Counter):
    def sum(self):
        return sum(self.values())

    def __add__(self, other):
        return StatusDistribution(super().__add__(other))

    @classmethod
    def from_status(cls, status):
        return cls({status: 1})


def print_table(scheduler, graph, targets):
    targets = list(targets)

    name_col_width = max((len(target.name) for target in targets), default=0) + 4

    status_dists = {}

    def make_status_distribution(node):
        if node not in status_dists:
            status = scheduler.status(target)
            status_dists[node] = sum(
                (make_status_distribution(dep) for dep in graph.dependencies[node]),
                StatusDistribution.from_status(status),
            )
        return status_dists[node]

    sorted_targets = sorted(targets, key=lambda t: t.order)
    target_metas = get_target_metas(sorted_targets)
    for target, meta in zip(sorted_targets, target_metas):
        status = scheduler.status(target)

        status_dist = make_status_distribution(target)
        percentage = status_dist[TargetStatus.COMPLETED] / status_dist.sum()

        runtime = meta.runtime()
        if runtime is None or meta.state in TargetStatus.SHOULDRUN_STATES:
            duration = "--:--:--"
        else:
            m, s = divmod(runtime, 60)
            h, m = divmod(m, 60)
            duration = "{:02.0f}:{:02.0f}:{:02.0f}".format(h, m, s)

        line = TABLE_FORMAT.format(
            name=target.name,
            status=click.style(STATUS_HUMAN[status], fg=STATUS_COLORS[status]),
            duration=duration,
            percentage=percentage,
            name_col_width=name_col_width,
        )
        click.echo(line)


def print_summary(backend, graph, targets):
    scheduler = Scheduler(backend=backend, graph=graph)
    status_counts = Counter(scheduler.status(target) for target in targets)
    click.echo("{:<15}{:>10}".format("total", len(targets)))
    for status in STATUS_ORDER:
        color = STATUS_COLORS[status]
        padded_name = "{:<15}".format(status)
        click.echo(
            "{}{:>10}".format(click.style(padded_name, fg=color), status_counts[status])
        )


@click.command()
@click.argument("targets", nargs=-1)
@click.option("--endpoints", is_flag=True, default=False, help="Show only endpoints.")
@click.option(
    "--summary", is_flag=True, default=False, help="Only show summary statistics."
)
@click.option(
    "-s",
    "--status",
    type=click.Choice(["shouldrun", "submitted", "running", "completed"]),
    multiple=True,
)
@click.pass_obj
def status(obj, status, summary, endpoints, targets):
    """
    Show the status of targets.

    One target per line is shown. Each line contains the target name, the
    status of the target itself, and the percentage of dependencies of the
    target that are completed (including the target itself). That is, 100%
    means that the target and all of its dependencies have been completed.

    In square brackets, the number of targets that should run, have been
    submitted, are running, and completed targets are shown, respectively.

    The `-s/--status` flag can be applied multiple times to show targets that
    match either of the queries, e.g. `gwf status -s shouldrun -s completed`
    will show all targets that should run and all targets that are completed.

    The targets are shown in creation-order.
    """
    graph = graph_from_config(obj)
    backend_cls = backend_from_config(obj)

    with backend_cls() as backend:
        scheduler = Scheduler(graph=graph, backend=backend)

        filters = []
        if status:
            filters.append(StatusFilter(scheduler=scheduler, status=status))
        if targets:
            filters.append(NameFilter(patterns=targets))
        if endpoints:
            filters.append(EndpointFilter(endpoints=graph.endpoints()))

        matches = filter_generic(targets=graph, filters=filters)

        if not summary:
            print_table(scheduler, graph, matches)
        else:
            print_summary(scheduler, graph, matches)
