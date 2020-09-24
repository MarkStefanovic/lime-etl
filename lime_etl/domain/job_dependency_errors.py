import dataclasses
import typing

from lime_etl.domain import value_objects


@dataclasses.dataclass(frozen=True, order=True)
class JobDependencyErrors:
    job_name: value_objects.JobName
    missing_dependencies: typing.FrozenSet[value_objects.JobName]
    jobs_out_of_order: typing.FrozenSet[value_objects.JobName]

    def __str__(self) -> str:
        if self.missing_dependencies:
            missing_deps_str: typing.Optional[str] = (
                f" has the following unresolved dependencies: "
                f"{', '.join(sorted('[' + dep.value + ']' for dep in self.missing_dependencies))}"
            )
        else:
            missing_deps_str = None

        if self.jobs_out_of_order:
            jobs_out_of_order_str: typing.Optional[str] = (
                f" depends on the following jobs which come after it: "
                f"{', '.join(sorted('[' + dep.value + ']'  for dep in self.jobs_out_of_order))}"
            )
        else:
            jobs_out_of_order_str = None

        if missing_deps_str and jobs_out_of_order_str:
            return f"[{self.job_name.value}]{missing_deps_str}.  It also {jobs_out_of_order_str}."
        elif missing_deps_str:
            return f"[{self.job_name.value}]{missing_deps_str}."
        elif jobs_out_of_order_str:
            return f"[{self.job_name.value}]{jobs_out_of_order_str}."
        else:
            raise ValueError(
                "There are no missing dependencies or jobs out of order, so there is nothing to render."
            )
