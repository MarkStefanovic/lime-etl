import abc
import typing

import lime_uow as lu

from lime_etl.domain import batch_status, value_objects

__all__ = ("BatchRepository",)


class BatchRepository(lu.Repository[batch_status.BatchStatusDTO], abc.ABC):
    @abc.abstractmethod
    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep, /) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def get_latest(self) -> typing.Optional[batch_status.BatchStatusDTO]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_previous(self) -> typing.Optional[batch_status.BatchStatusDTO]:
        raise NotImplementedError
