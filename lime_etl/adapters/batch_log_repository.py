import abc
import datetime

import sqlalchemy as sa
import sqlalchemy.orm as sao

from adapters import timestamp_adapter  # type: ignore
from domain import batch_log_entry, value_objects  # type: ignore


class BatchLogRepository(abc.ABC):
    @abc.abstractmethod
    def add(
        self, log_entry: batch_log_entry.BatchLogEntry
    ) -> batch_log_entry.BatchLogEntry:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def get_earliest(self) -> batch_log_entry.BatchLogEntry:
        raise NotImplementedError

    @abc.abstractmethod
    def get_latest(self) -> batch_log_entry.BatchLogEntry:
        raise NotImplementedError


class SqlAlchemyBatchLogRepository(BatchLogRepository):
    def __init__(
        self,
        session: sao.Session,
        ts_adapter: timestamp_adapter.TimestampAdapter,
    ):
        self._session = session
        self._ts_adapter = ts_adapter

    def add(
        self, log_entry: batch_log_entry.BatchLogEntry
    ) -> batch_log_entry.BatchLogEntry:
        log_entry_dto = log_entry.to_dto()
        self._session.add(log_entry_dto)
        return log_entry

    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        ts = self._ts_adapter.now().value
        cutoff = ts - datetime.timedelta(days=days_to_keep.value)
        return (
            self._session.query(batch_log_entry.BatchLogEntryDTO)
            .filter(batch_log_entry.BatchLogEntryDTO.ts < cutoff)
            .delete()
        )

    def get_earliest(self) -> batch_log_entry.BatchLogEntry:
        dto: batch_log_entry.BatchLogEntryDTO = (
            self._session.query(batch_log_entry.BatchLogEntryDTO)
            .order_by(batch_log_entry.BatchLogEntryDTO.ts)
            .first()
        )
        return dto.to_domain()

    def get_latest(self) -> batch_log_entry.BatchLogEntry:
        dto: batch_log_entry.BatchLogEntryDTO = (
            self._session.query(batch_log_entry.BatchLogEntryDTO)
            .order_by(sa.desc(batch_log_entry.BatchLogEntryDTO.ts))
            .first()
        )
        return dto.to_domain()
