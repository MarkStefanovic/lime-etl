from __future__ import annotations

import abc
import datetime
import typing

import lime_uow as lu

from lime_etl.domain import batch_log_entry, value_objects

__all__ = ("BatchLogRepository",)


class BatchLogRepository(
    lu.Repository[batch_log_entry.BatchLogEntryDTO],
    abc.ABC,
):
    @staticmethod
    def key() -> str:
        return BatchLogRepository.__name__

    @abc.abstractmethod
    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def get_earliest_timestamp(self) -> typing.Optional[datetime.datetime]:
        raise NotImplementedError
