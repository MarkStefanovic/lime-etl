import abc
import typing

import lime_uow as lu
from sqlalchemy import desc, orm

from lime_etl.adapters import timestamp_adapter
from lime_etl.domain import job_result, value_objects


__all__ = (
    "JobRepository",
    "SqlAlchemyJobRepository",
)


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


class SqlAlchemyJobRepository(
    JobRepository, lu.SqlAlchemyRepository[job_result.JobResultDTO]
):
    def __init__(
        self,
        session: orm.Session,
        ts_adapter: timestamp_adapter.TimestampAdapter,
    ):
        self._ts_adapter = ts_adapter
        super().__init__(session)

    @property
    def entity_type(self) -> typing.Type[job_result.JobResultDTO]:
        return job_result.JobResultDTO

    def get_latest(
        self, job_name: value_objects.JobName, /
    ) -> typing.Optional[job_result.JobResultDTO]:
        # noinspection PyTypeChecker
        return (
            self.session.query(job_result.JobResultDTO)
            .order_by(desc(job_result.JobResultDTO.ts))  # type: ignore
            .first()
        )

    def get_last_successful_ts(
        self, job_name: value_objects.JobName, /
    ) -> typing.Optional[value_objects.Timestamp]:
        # noinspection PyUnresolvedReferences,PyTypeChecker
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

    @classmethod
    def interface(cls) -> typing.Type[JobRepository]:
        return JobRepository
