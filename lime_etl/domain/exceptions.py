import typing

from lime_etl.domain import job_dependency_errors, value_objects


class LimeETLException(Exception):
    """Base class for exceptions arising from the lime-etl package"""

    def __init__(self, message: str, /):
        self.message = message
        super().__init__(message)


class BatchNotFound(LimeETLException):
    def __init__(
        self,
        batch_id: value_objects.UniqueId,
        /,
    ):
        self.batch_id = batch_id
        msg = f"The batch [{batch_id.value}] was not found"
        super().__init__(msg)


class DependencyErrors(LimeETLException):
    def __init__(
        self,
        dependency_errors: typing.Set[job_dependency_errors.JobDependencyErrors],
        /,
    ):
        self.dependency_errors = dependency_errors
        # noinspection PyTypeChecker
        msg = "; ".join(str(e) for e in sorted(dependency_errors))
        super().__init__(msg)


class DuplicateJobNamesError(LimeETLException):
    def __init__(
        self,
        duplicate_job_counts: typing.Dict[value_objects.JobName, int],
        /,
    ):
        self.duplicate_job_counts = duplicate_job_counts
        dupes_msg = ", ".join(
            f"[{job_name.value}] ({ct})"
            for job_name, ct in duplicate_job_counts.items()
        )
        err_msg = f"The following job names included more than once: {dupes_msg}."
        super().__init__(err_msg)


class InvalidBatch(LimeETLException):
    def __init__(
        self,
        message: str,
        /,
    ):
        super().__init__(message)


class InvalidJobSpec(LimeETLException):
    def __init__(
        self,
        message: str,
        /,
    ):
        super().__init__(message)


class InvalidResource(LimeETLException):
    def __init__(self, message: str, /):
        super().__init__(message)


class MissingResourcesError(LimeETLException):
    def __init__(
        self,
        missing_resources: typing.Mapping[
            value_objects.JobName, typing.Iterable[value_objects.ResourceName]
        ],
    ):
        self.missing_resources = missing_resources
        messages = "; ".join(
            f"[{job_name}] is missing {', '.join(sorted(f'[{m}]' for m in missing))}"
            for job_name, missing in missing_resources.items()
        )
        msg = f"The following jobs have missing resources: {messages}."
        super().__init__(msg)
