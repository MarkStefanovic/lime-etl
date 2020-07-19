import abc
import datetime

from sqlalchemy.orm import Session

from src.adapters import timestamp_adapter
from src.domain import batch_log_entry, value_objects


class BatchLogRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, log_entry: batch_log_entry.BatchLogEntry) -> batch_log_entry.BatchLogEntry:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        raise NotImplementedError


class SqlAlchemyBatchLogRepository(BatchLogRepository):
    def __init__(
        self, session: Session, ts_adapter: timestamp_adapter.TimestampAdapter,
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
