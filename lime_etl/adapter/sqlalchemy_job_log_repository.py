import datetime
import typing

from lime_uow import sqlalchemy_resources as lsa
from sqlalchemy import orm

from lime_etl import domain


__all__ = ("SqlAlchemyJobLogRepository",)


class SqlAlchemyJobLogRepository(
    domain.JobLogRepository,
    lsa.SqlAlchemyRepository[domain.JobLogEntryDTO],
):
    def __init__(
        self,
        session: orm.Session,
        ts_adapter: domain.TimestampAdapter,
    ):
        self._ts_adapter = ts_adapter
        super().__init__(session)

    @staticmethod
    def key() -> str:
        return domain.JobLogRepository.__name__

    def delete_old_entries(self, days_to_keep: domain.DaysToKeep) -> int:
        ts = self._ts_adapter.now().value
        cutoff = ts - datetime.timedelta(days=days_to_keep.value)
        return (
            self.session.query(domain.JobLogEntryDTO)
            .filter(domain.JobLogEntryDTO.ts < cutoff)
            .delete()
        )

    @property
    def entity_type(self) -> typing.Type[domain.JobLogEntryDTO]:
        return domain.JobLogEntryDTO
