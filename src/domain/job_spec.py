from __future__ import annotations

import abc
from typing import Iterable, List


from src.domain import job_test_result, value_objects
from src.services import job_logging_service, unit_of_work


class JobSpec(abc.ABC):
    @property
    @abc.abstractmethod
    def dependencies(self) -> List[JobSpec]:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def flex_pct(self) -> value_objects.FlexPercent:
        raise NotImplementedError

    @property
    def job_name(self) -> value_objects.JobName:
        return value_objects.JobName(value=self.__class__.__name__)

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

    @abc.abstractmethod
    def run(self, logger: job_logging_service.JobLoggingService,) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def test(
        self, logger: job_logging_service.JobLoggingService,
    ) -> Iterable[job_test_result.JobTestResult]:
        raise NotImplementedError
