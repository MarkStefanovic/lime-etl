import abc
import datetime
import typing

import lime_uow as lu
from sqlalchemy import orm

from lime_etl.adapters import timestamp_adapter
from lime_etl.domain import job_log_entry, value_objects


__all__ = (
    "JobLogRepository",
    "SqlAlchemyJobLogRepository",
)


class JobLogRepository(
    lu.Repository[job_log_entry.JobLogEntryDTO],
    abc.ABC,
):
    @abc.abstractmethod
    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        raise NotImplementedError


class SqlAlchemyJobLogRepository(
    JobLogRepository,
    lu.SqlAlchemyRepository[job_log_entry.JobLogEntryDTO],
):
    def __init__(
        self,
        session: orm.Session,
        ts_adapter: timestamp_adapter.TimestampAdapter,
    ):
        self._ts_adapter = ts_adapter
        super().__init__(session)

    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        ts = self._ts_adapter.now().value
        cutoff = ts - datetime.timedelta(days=days_to_keep.value)
        return (
            self.session.query(job_log_entry.JobLogEntryDTO)
            .filter(job_log_entry.JobLogEntryDTO.ts < cutoff)
            .delete()
        )

    @property
    def entity_type(self) -> typing.Type[job_log_entry.JobLogEntryDTO]:
        return job_log_entry.JobLogEntryDTO

    @classmethod
    def interface(cls) -> typing.Type[JobLogRepository]:
        return JobLogRepository
