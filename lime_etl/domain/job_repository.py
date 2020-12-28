import abc
import typing

import lime_uow as lu

from lime_etl.domain import job_result, job_status, job_test_result, value_objects


__all__ = ("JobRepository",)


class JobRepository(lu.Repository[job_result.JobResultDTO], abc.ABC):
    @abc.abstractmethod
    def last_job_run_status(
        self, /, job_name: value_objects.JobName
    ) -> typing.Optional[job_status.JobStatus]:
        raise NotImplementedError

    @abc.abstractmethod
    def latest_test_results(
        self, /, job_name: value_objects.JobName,
    ) -> typing.FrozenSet[job_test_result.JobTestResult]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_last_successful_ts(
        self, /, job_name: value_objects.JobName
    ) -> typing.Optional[value_objects.Timestamp]:
        raise NotImplementedError
