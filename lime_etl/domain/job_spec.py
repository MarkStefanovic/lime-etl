from __future__ import annotations

import abc
import typing

import lime_uow as lu

from lime_etl.domain import admin_unit_of_work, job_logger, job_status, job_test_result, value_objects

__all__ = ("JobSpec",)

UoW = typing.TypeVar("UoW", bound=lu.UnitOfWork, contravariant=True)


class JobSpec(abc.ABC, typing.Generic[UoW]):
    @property
    def dependencies(self) -> typing.Tuple[value_objects.JobName, ...]:
        return tuple()

    @property
    @abc.abstractmethod
    def job_name(self) -> value_objects.JobName:
        raise NotImplementedError

    @property
    def min_seconds_between_refreshes(self) -> value_objects.MinSecondsBetweenRefreshes:
        return value_objects.MinSecondsBetweenRefreshes(0)

    @property
    def min_seconds_between_tests(self) -> value_objects.MinSecondsBetweenTests:
        return value_objects.MinSecondsBetweenTests(0)

    @property
    def max_retries(self) -> value_objects.MaxRetries:
        return value_objects.MaxRetries(0)

    def on_execution_error(self, error_message: str) -> typing.Optional[JobSpec[UoW]]:
        return None

    def on_test_failure(
        self, test_results: typing.FrozenSet[job_test_result.JobTestResult]
    ) -> typing.Optional[JobSpec[UoW]]:
        return None

    @abc.abstractmethod
    def run(
        self,
        admin_uow: admin_unit_of_work.AdminUnitOfWork,
        batch_uow: UoW,
        logger: job_logger.JobLogger,
    ) -> job_status.JobStatus:
        raise NotImplementedError

    @abc.abstractmethod
    def test(
        self,
        admin_uow: admin_unit_of_work.AdminUnitOfWork,
        batch_uow: UoW,
        logger: job_logger.JobLogger,
    ) -> typing.Collection[job_test_result.SimpleJobTestResult]:
        raise NotImplementedError

    @property
    def timeout_seconds(self) -> value_objects.TimeoutSeconds:
        return value_objects.TimeoutSeconds(None)

    def __repr__(self) -> str:
        return f"<JobSpec: {self.__class__.__name__}>: {self.job_name.value}"

    def __hash__(self) -> int:
        return hash(self.job_name.value)

    def __eq__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return (
                self.job_name.value == typing.cast(JobSpec[UoW], other).job_name.value
            )
        else:
            return NotImplemented
