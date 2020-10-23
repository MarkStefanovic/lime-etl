from __future__ import annotations

import abc
import typing

import lime_uow as lu

from lime_etl.domain import job_test_result, value_objects
from lime_etl.services import job_logging_service


class JobSpec(abc.ABC):
    @property
    @abc.abstractmethod
    def dependencies(self) -> typing.List[value_objects.JobName]:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def job_name(self) -> value_objects.JobName:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def max_retries(self) -> value_objects.MaxRetries:
        raise NotImplementedError

    def on_execution_error(self, error_message: str) -> typing.Optional[JobSpec]:
        return None

    def on_test_failure(
        self, test_results: typing.FrozenSet[job_test_result.JobTestResult]
    ) -> typing.Optional[JobSpec]:
        return None

    @abc.abstractmethod
    def run(
        self,
        logger: job_logging_service.AbstractJobLoggingService,
        batch_uow: lu.UnitOfWork,
    ) -> value_objects.Result:
        raise NotImplementedError

    @abc.abstractmethod
    def test(
        self,
        logger: job_logging_service.AbstractJobLoggingService,
        batch_uow: lu.UnitOfWork,
    ) -> typing.Collection[job_test_result.SimpleJobTestResult]:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def seconds_between_refreshes(self) -> value_objects.SecondsBetweenRefreshes:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def timeout_seconds(self) -> value_objects.TimeoutSeconds:
        raise NotImplementedError
