from __future__ import annotations

import abc
import typing

import lime_uow as lu

from lime_etl import domain
from lime_etl.services import job_logging_service

__all__ = ("JobSpec",)

UoW = typing.TypeVar("UoW", bound=lu.UnitOfWork, contravariant=True)


class JobSpec(abc.ABC, typing.Generic[UoW]):
    @property
    def dependencies(self) -> typing.Tuple[domain.JobName, ...]:
        return tuple()

    @property
    @abc.abstractmethod
    def job_name(self) -> domain.JobName:
        raise NotImplementedError

    @property
    def max_retries(self) -> domain.MaxRetries:
        return domain.MaxRetries(0)

    def on_execution_error(self, error_message: str) -> typing.Optional[JobSpec[UoW]]:
        return None

    def on_test_failure(
        self, test_results: typing.FrozenSet[domain.JobTestResult]
    ) -> typing.Optional[JobSpec[UoW]]:
        return None

    @abc.abstractmethod
    def run(
        self,
        uow: UoW,
        logger: job_logging_service.AbstractJobLoggingService,
    ) -> domain.JobStatus:
        raise NotImplementedError

    @abc.abstractmethod
    def test(
        self,
        uow: UoW,
        logger: job_logging_service.AbstractJobLoggingService,
    ) -> typing.Collection[domain.SimpleJobTestResult]:
        raise NotImplementedError

    @property
    def min_seconds_between_refreshes(self) -> domain.MinSecondsBetweenRefreshes:
        return domain.MinSecondsBetweenRefreshes(0)

    @property
    def timeout_seconds(self) -> domain.TimeoutSeconds:
        return domain.TimeoutSeconds(None)

    def __repr__(self) -> str:
        return f"<JobSpec: {self.__class__.__name__}>: {self.job_name.value}"

    def __hash__(self) -> int:
        return hash(self.job_name.value)

    def __eq__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return self.job_name.value == typing.cast(JobSpec[UoW], other).job_name.value
        else:
            return NotImplemented
