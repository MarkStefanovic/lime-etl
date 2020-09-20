from __future__ import annotations

import abc

import typing

from lime_etl.domain import job_test_result, value_objects
from lime_etl.services import job_logging_service, unit_of_work


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
    def seconds_between_refreshes(self) -> value_objects.SecondsBetweenRefreshes:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def timeout_seconds(self) -> value_objects.TimeoutSeconds:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def max_retries(self) -> value_objects.MaxRetries:
        raise NotImplementedError


class AdminJobSpec(JobSpec):
    @abc.abstractmethod
    def run(
        self,
        uow: unit_of_work.UnitOfWork,
        logger: job_logging_service.JobLoggingService,
    ) -> value_objects.Result:
        raise NotImplementedError

    @abc.abstractmethod
    def test(
        self,
        uow: unit_of_work.UnitOfWork,
        logger: job_logging_service.JobLoggingService,
    ) -> typing.Collection[job_test_result.SimpleJobTestResult]:
        raise NotImplementedError


class ETLJobSpec(JobSpec):
    @abc.abstractmethod
    def run(
        self, logger: job_logging_service.JobLoggingService
    ) -> value_objects.Result:
        raise NotImplementedError

    @abc.abstractmethod
    def test(
        self, logger: job_logging_service.JobLoggingService
    ) -> typing.Collection[job_test_result.SimpleJobTestResult]:
        raise NotImplementedError
