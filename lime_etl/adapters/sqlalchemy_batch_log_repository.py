import datetime
import typing

from lime_uow import sqlalchemy_resources as lsa
from sqlalchemy import orm

from lime_etl import domain


__all__ = ("SqlAlchemyBatchLogRepository",)


class SqlAlchemyBatchLogRepository(
    domain.BatchLogRepository,
    lsa.SqlAlchemyRepository[domain.BatchLogEntryDTO],
):
    def __init__(
        self,
        session: orm.Session,
        ts_adapter: domain.TimestampAdapter,
    ):
        self._ts_adapter = ts_adapter
        super().__init__(session)

    def delete_old_entries(self, days_to_keep: domain.DaysToKeep) -> int:
        ts = self._ts_adapter.now().value
        cutoff = ts - datetime.timedelta(days=days_to_keep.value)
        return (
            self.session.query(domain.BatchLogEntryDTO)
            .filter(domain.BatchLogEntryDTO.ts < cutoff)
            .delete()
        )

    @property
    def entity_type(self) -> typing.Type[domain.BatchLogEntryDTO]:
        return domain.BatchLogEntryDTO

    def get_earliest_timestamp(self) -> typing.Optional[datetime.datetime]:
        earliest_entry = (
            self.session.query(domain.BatchLogEntryDTO)
            .order_by(domain.BatchLogEntryDTO.ts)
            .first()
        )
        if earliest_entry is None:
            return None
        else:
            return earliest_entry.ts

    @classmethod
    def interface(cls) -> typing.Type[domain.BatchLogRepository]:
        return domain.BatchLogRepository
