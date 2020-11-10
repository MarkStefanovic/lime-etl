from __future__ import annotations

import abc
import typing

from lime_etl import domain
from lime_etl.services import job_logging_service

import lime_uow as lu


__all__ = ("JobSpec",)


class JobSpec(abc.ABC):
    def __init__(
        self,
        job_name: domain.JobName,
        dependencies: typing.Collection[domain.JobName] = tuple(),
        job_id: typing.Optional[domain.UniqueId] = None,
        max_retries: domain.MaxRetries = domain.MaxRetries(0),
        min_seconds_between_refreshes: domain.MinSecondsBetweenRefreshes = domain.MinSecondsBetweenRefreshes(
            300
        ),
        timeout_seconds: domain.TimeoutSeconds = domain.TimeoutSeconds(None),
    ):
        self._job_name = job_name
        self._dependencies = tuple(dependencies)
        self._job_id: typing.Optional[domain.UniqueId] = job_id
        self._max_retries = max_retries
        self._min_seconds_between_refreshes = min_seconds_between_refreshes
        self._timeout_seconds = timeout_seconds

    @property
    def dependencies(self) -> typing.Tuple[domain.JobName, ...]:
        return self._dependencies

    @property
    def job_id(self) -> domain.UniqueId:
        if self._job_id is None:
            self._job_id = domain.UniqueId.generate()
        return self._job_id

    @property
    def job_name(self) -> domain.JobName:
        return self._job_name

    @property
    def max_retries(self) -> domain.MaxRetries:
        return self._max_retries

    def on_execution_error(self, error_message: str) -> typing.Optional[JobSpec]:
        return None

    def on_test_failure(
        self, test_results: typing.FrozenSet[domain.JobTestResult]
    ) -> typing.Optional[JobSpec]:
        return None

    @abc.abstractmethod
    def run(
        self,
        uow: lu.UnitOfWork,
        logger: job_logging_service.AbstractJobLoggingService,
    ) -> domain.Result:
        raise NotImplementedError

    @abc.abstractmethod
    def test(
        self,
        uow: lu.UnitOfWork,
        logger: job_logging_service.AbstractJobLoggingService,
    ) -> typing.Collection[domain.SimpleJobTestResult]:
        raise NotImplementedError

    @property
    def min_seconds_between_refreshes(self) -> domain.MinSecondsBetweenRefreshes:
        return self._min_seconds_between_refreshes

    @property
    def timeout_seconds(self) -> domain.TimeoutSeconds:
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
