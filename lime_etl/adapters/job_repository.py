import abc
import datetime

import typing
from sqlalchemy import desc
from sqlalchemy.orm import Session

from lime_etl.adapters import timestamp_adapter
from lime_etl.domain import job_result, value_objects


class JobRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, job: job_result.JobResult, /) -> job_result.JobResult:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep, /) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_latest(
        self, job_name: value_objects.JobName, /
    ) -> typing.Optional[job_result.JobResult]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_last_successful_ts(
        self, job_name: value_objects.JobName, /
    ) -> typing.Optional[value_objects.Timestamp]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_job_by_id(self, job_id: value_objects.UniqueId, /) -> job_result.JobResult:
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, job_to_update: job_result.JobResult, /) -> job_result.JobResult:
        raise NotImplementedError


class SqlAlchemyJobRepository(JobRepository):
    def __init__(
        self,
        session: Session,
        ts_adapter: timestamp_adapter.TimestampAdapter,
    ):
        self._session = session
        self._ts_adapter = ts_adapter
        super().__init__()

    def add(
        self,
        new_job: job_result.JobResult,
        /,
    ) -> typing.Optional[job_result.JobResult]:
        dto = new_job.to_dto()
        self._session.add(dto)
        return new_job

    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep, /) -> None:
        ts = self._ts_adapter.now().value
        cutoff: datetime.datetime = ts - datetime.timedelta(days=days_to_keep.value)
        return (
            self._session.query(job_result.JobResultDTO)
            .filter(job_result.JobResultDTO.ts < cutoff)
            .delete(synchronize_session=False)
        )

    def get_job_by_id(
        self, job_id: value_objects.UniqueId, /
    ) -> typing.Optional[job_result.JobResult]:
        result: typing.Optional[job_result.JobResultDTO] = (
            self._session.query(job_result.JobResultDTO)
            .filter(job_result.JobResultDTO.id == job_id.value)
            .first()
        )
        if result is None:
            return None
        else:
            return result.to_domain()

    def get_latest(
        self, job_name: value_objects.JobName, /
    ) -> typing.Optional[job_result.JobResult]:
        result: typing.Optional[job_result.JobResultDTO] = (
            self._session.query(job_result.JobResultDTO)
            .order_by(desc(job_result.JobResultDTO.ts))  # type: ignore
            .first()
        )
        if result is None:
            return None
        else:
            return result.to_domain()

    def get_last_successful_ts(
        self, job_name: value_objects.JobName, /
    ) -> typing.Optional[value_objects.Timestamp]:
        # noinspection PyUnresolvedReferences
        jr: typing.Optional[job_result.JobResultDTO] = (
            self._session.query(job_result.JobResultDTO)
            .filter(job_result.JobResultDTO.job_name.ilike(job_name.value))  # type: ignore
            .filter(job_result.JobResultDTO.execution_error_occurred.is_(False))  # type: ignore
            .order_by(desc(job_result.JobResultDTO.ts))  # type: ignore
            .first()
        )
        if jr is None:
            return None
        else:
            return value_objects.Timestamp(jr.ts)

    def update(self, job_to_update: job_result.JobResult, /) -> job_result.JobResult:
        dto = job_to_update.to_dto()
        self._session.merge(dto)
        return job_to_update
