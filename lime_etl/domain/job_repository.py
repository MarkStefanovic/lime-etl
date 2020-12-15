import abc
import typing

import lime_uow as lu

from lime_etl.domain import job_result, value_objects


__all__ = ("JobRepository",)


class JobRepository(lu.Repository[job_result.JobResultDTO], abc.ABC):
    @abc.abstractmethod
    def get_latest(
        self, job_name: value_objects.JobName, /
    ) -> typing.Optional[job_result.JobResultDTO]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_last_successful_ts(
        self, job_name: value_objects.JobName, /
    ) -> typing.Optional[value_objects.Timestamp]:
        raise NotImplementedError
