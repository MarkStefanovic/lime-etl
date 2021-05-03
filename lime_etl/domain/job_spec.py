from __future__ import annotations

import abc
import typing

import lime_uow as lu

from lime_etl.domain import (
    job_logger,
    job_status,
    job_test_result,
    value_objects,
)

__all__ = ("JobSpec", "SimpleJobSpec")


class JobSpec(abc.ABC):
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

    def on_execution_error(self, error_message: str) -> typing.Optional[JobSpec]:
        return None

    def on_test_failure(
        self, test_results: typing.FrozenSet[job_test_result.JobTestResult]
    ) -> typing.Optional[JobSpec]:
        return None

    @abc.abstractmethod
    def run(
        self,
        uow: lu.UnitOfWork,
        logger: job_logger.JobLogger,
    ) -> job_status.JobStatus:
        raise NotImplementedError

    def test(
        self,
        uow: lu.UnitOfWork,
        logger: job_logger.JobLogger,
    ) -> typing.List[job_test_result.SimpleJobTestResult]:
        return []

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
                self.job_name.value == typing.cast(JobSpec, other).job_name.value
            )
        else:
            return NotImplemented


class SimpleJobSpec(JobSpec):
    def __init__(
        # fmt: off
            self,
            *,
            name: str,
            dependencies: typing.Optional[typing.Set[str]] = None,
            timeout_seconds: typing.Optional[int] = None,
            max_retries: typing.Optional[int] = None,
            min_seconds_between_refreshes: typing.Optional[int] = None,
            min_seconds_between_tests: typing.Optional[int] = None,
        # fmt: on
    ):
        self._name = value_objects.JobName(name)
        self._dependencies = tuple(value_objects.JobName(d) for d in dependencies or [])
        self._timeout_seconds = value_objects.TimeoutSeconds(timeout_seconds)
        self._max_retries = value_objects.MaxRetries(int(max_retries or 0))
        self._min_seconds_between_refreshes = value_objects.MinSecondsBetweenRefreshes(
            int(min_seconds_between_refreshes or 0)
        )
        self._min_seconds_between_tests = value_objects.MinSecondsBetweenTests(
            int(min_seconds_between_tests or 0)
        )

    @property
    def dependencies(self) -> typing.Tuple[value_objects.JobName, ...]:
        return self._dependencies

    @property
    def job_name(self) -> value_objects.JobName:
        return self._name

    @property
    def min_seconds_between_refreshes(self) -> value_objects.MinSecondsBetweenRefreshes:
        return self._min_seconds_between_refreshes

    @property
    def min_seconds_between_tests(self) -> value_objects.MinSecondsBetweenTests:
        return self._min_seconds_between_tests

    @property
    def max_retries(self) -> value_objects.MaxRetries:
        return self._max_retries

    @abc.abstractmethod
    def run(
        self,
        uow: lu.UnitOfWork,
        logger: job_logger.JobLogger,
    ) -> job_status.JobStatus:
        raise NotImplementedError

    def test(
        self,
        uow: lu.UnitOfWork,
        logger: job_logger.JobLogger,
    ) -> typing.List[job_test_result.SimpleJobTestResult]:
        return []

    @property
    def timeout_seconds(self) -> value_objects.TimeoutSeconds:
        return self._timeout_seconds
