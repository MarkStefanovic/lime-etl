from __future__ import annotations

import abc
import typing

from lime_etl.domain import job_test_result, value_objects
from lime_etl.services import job_logging_service


class JobSpec(abc.ABC):
    def __init__(
        self,
        job_name: value_objects.JobName,
        dependencies: typing.Collection[value_objects.JobName] = tuple(),
        job_id: typing.Optional[value_objects.UniqueId] = None,
        max_retries: value_objects.MaxRetries = value_objects.MaxRetries(0),
        min_seconds_between_refreshes: value_objects.MinSecondsBetweenRefreshes = value_objects.MinSecondsBetweenRefreshes(300),
        timeout_seconds: value_objects.TimeoutSeconds = value_objects.TimeoutSeconds(None),
    ):
        self._job_name = job_name
        self._dependencies = tuple(dependencies)
        self._job_id: typing.Optional[value_objects.UniqueId] = job_id
        self._max_retries = max_retries
        self._min_seconds_between_refreshes = min_seconds_between_refreshes
        self._timeout_seconds = timeout_seconds

    @property
    def dependencies(self) -> typing.Tuple[value_objects.JobName, ...]:
        return self._dependencies

    @property
    def job_id(self) -> value_objects.UniqueId:
        if self._job_id is None:
            self._job_id = value_objects.UniqueId.generate()
        return self._job_id

    @property
    def job_name(self) -> value_objects.JobName:
        return self._job_name

    @property
    def max_retries(self) -> value_objects.MaxRetries:
        return self._max_retries

    def on_execution_error(self, error_message: str) -> typing.Optional[JobSpec]:
        return None

    def on_test_failure(
        self, test_results: typing.FrozenSet[job_test_result.JobTestResult]
    ) -> typing.Optional[JobSpec]:
        return None

    @abc.abstractmethod
    def run(
        self,
        /,
        logger: job_logging_service.AbstractJobLoggingService,
    ) -> value_objects.Result:
        raise NotImplementedError

    @abc.abstractmethod
    def test(
        self,
        /,
        logger: job_logging_service.AbstractJobLoggingService,
    ) -> typing.Collection[job_test_result.SimpleJobTestResult]:
        raise NotImplementedError

    @property
    def min_seconds_between_refreshes(self) -> value_objects.MinSecondsBetweenRefreshes:
        return self._min_seconds_between_refreshes

    @property
    def timeout_seconds(self) -> value_objects.TimeoutSeconds:
        return self._timeout_seconds

    def __repr__(self) -> str:
        return f"<JobSpec: {self.__class__.__name__}>: {self.job_name.value}"

    def __hash__(self) -> int:
        return hash(self.job_name.value)

    def __eq__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return self.job_name.value == typing.cast(JobSpec, other).job_name.value
        else:
            return NotImplemented
