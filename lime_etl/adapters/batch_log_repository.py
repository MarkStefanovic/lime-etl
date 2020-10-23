from __future__ import annotations

import abc
import datetime
import typing

import lime_uow as lu
from sqlalchemy import orm

from lime_etl.adapters import timestamp_adapter
from lime_etl.domain import batch_log_entry, value_objects


class BatchLogRepository(
    lu.Repository[batch_log_entry.BatchLogEntryDTO],
    abc.ABC,
):
    @abc.abstractmethod
    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def get_earliest_timestamp(self) -> typing.Optional[datetime.datetime]:
        raise NotImplementedError


class SqlAlchemyBatchLogRepository(
    BatchLogRepository,
    lu.SqlAlchemyRepository[batch_log_entry.BatchLogEntryDTO],
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
            self.session.query(batch_log_entry.BatchLogEntryDTO)
            .filter(batch_log_entry.BatchLogEntryDTO.ts < cutoff)
            .delete()
        )

    @property
    def entity_type(self) -> typing.Type[batch_log_entry.BatchLogEntryDTO]:
        return batch_log_entry.BatchLogEntryDTO

    def get_earliest_timestamp(self) -> typing.Optional[datetime.datetime]:
        earliest_entry = (
            self.session.query(batch_log_entry.BatchLogEntryDTO)
            .order_by(batch_log_entry.BatchLogEntryDTO.ts)
            .first()
        )
        if earliest_entry is None:
            return None
        else:
            return earliest_entry.ts

    @classmethod
    def interface(cls) -> typing.Type[BatchLogRepository]:
        return BatchLogRepository
