import abc
import datetime
import typing

import lime_uow as lu
import sqlalchemy as sa
from sqlalchemy.orm import Session

from lime_etl.adapters import timestamp_adapter
from lime_etl.domain import batch_status, value_objects


__all__ = (
    "BatchRepository",
    "SqlAlchemyBatchRepository",
)


class BatchRepository(lu.Repository[batch_status.BatchStatusDTO], abc.ABC):
    @abc.abstractmethod
    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep, /) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def get_latest(self) -> typing.Optional[batch_status.BatchStatusDTO]:
        raise NotImplementedError


class SqlAlchemyBatchRepository(
    BatchRepository, lu.SqlAlchemyRepository[batch_status.BatchStatusDTO]
):
    def __init__(
        self,
        session: Session,
        ts_adapter: timestamp_adapter.TimestampAdapter,
    ):
        super().__init__(session)
        self._ts_adapter = ts_adapter

    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep, /) -> int:
        ts = self._ts_adapter.now().value
        cutoff: datetime.datetime = ts - datetime.timedelta(days=days_to_keep.value)
        # We need to delete batches one by one to trigger cascade deletes, a bulk update will
        # not trigger them, and we don't want to rely on specific database implementations, so
        # we cannot use ondelete='CASCADE' on the foreign key columns.
        batches: typing.List[batch_status.BatchStatusDTO] = (
            self.session.query(batch_status.BatchStatusDTO)
            .filter(batch_status.BatchStatusDTO.ts < cutoff)
            .all()
        )
        for b in batches:
            self.session.delete(b)
        return len(batches)

    @property
    def entity_type(self) -> typing.Type[batch_status.BatchStatusDTO]:
        return batch_status.BatchStatusDTO

    def get_latest(self) -> typing.Optional[batch_status.BatchStatusDTO]:
        # noinspection PyTypeChecker
        return (
            self.session.query(batch_status.BatchStatusDTO)
            .order_by(sa.desc(batch_status.BatchStatusDTO.ts))  # type: ignore
            .first()
        )

    @classmethod
    def interface(cls) -> typing.Type[BatchRepository]:
        return BatchRepository
