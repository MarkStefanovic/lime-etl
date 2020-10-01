import typing

from lime_etl.domain import job_dependency_errors, value_objects


class LimeETLException(Exception):
    """Base class for exceptions arising from the lime-etl package"""

    def __init__(self, message: str, /):
        self.message = message
        super().__init__(message)


class DependencyErrors(LimeETLException):
    def __init__(
        self,
        dependency_errors: typing.Set[job_dependency_errors.JobDependencyErrors],
        /,
    ):
        self.dependency_errors = dependency_errors

        msg = "; ".join(str(e) for e in sorted(dependency_errors))
        super().__init__(msg)


class MissingResourceError(LimeETLException):
    def __init__(
        self,
        job_name: value_objects.JobName,
        missing_resources: typing.Collection[value_objects.ResourceName],
    ):
        self.job_name = job_name
        self.missing_resources = missing_resources
        msg = (
            f"The job [{job_name.value}] requires the following resources, which were not found: "
            + ", ".join(f"[{r.value}]" for r in missing_resources)
            + "."
        )
        super().__init__(msg)
