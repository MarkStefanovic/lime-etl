import typing

from lime_etl.domain import job_dependency_errors, value_objects


class LimeETLException(Exception):
    """Base class for exceptions arising from the lime-etl package"""

    def __init__(self, message: str, /):
        self.message = message
        super().__init__(message)


class DependencyErrors(LimeETLException):
    def __init__(self, dependency_errors: typing.Set[job_dependency_errors.JobDependencyErrors], /):
        self.dependency_errors = dependency_errors

        msg = "\n".join(str(e) for e in sorted(dependency_errors))
        super().__init__(msg)
