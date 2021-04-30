import abc
import typing

import lime_uow as lu

from lime_etl.domain import batch_delta, batch_status, value_objects

__all__ = ("BatchRepository",)


class BatchRepository(lu.Repository[batch_status.BatchStatusDTO], abc.ABC):
    @staticmethod
    def key() -> str:
        return BatchRepository.__name__

    @abc.abstractmethod
    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep, /) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def get_latest(self, /, batch_name: value_objects.BatchName) -> typing.Optional[batch_status.BatchStatusDTO]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_latest_batch_delta(self, /, batch_name: value_objects.BatchName) -> batch_delta.BatchDelta:
        raise NotImplementedError
