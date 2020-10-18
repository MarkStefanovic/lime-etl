import abc
import datetime
import typing

from lime_uow import resources
from sqlalchemy import desc
from sqlalchemy.orm import Session

from lime_etl.adapters import timestamp_adapter
from lime_etl.domain import batch, value_objects


class BatchRepository(resources.Repository[batch.BatchDTO], abc.ABC):
    @abc.abstractmethod
    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep, /) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def get_latest(self) -> typing.Optional[batch.BatchDTO]:
        raise NotImplementedError


class SqlAlchemyBatchRepository(
    BatchRepository, resources.SqlAlchemyRepository[batch.BatchDTO]
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
        batches: typing.List[batch.BatchDTO] = (
            self.session.query(batch.BatchDTO).filter(batch.BatchDTO.ts < cutoff).all()
        )
        for b in batches:
            self.session.delete(b)
        return len(batches)

    @property
    def entity_type(self) -> typing.Type[batch.BatchDTO]:
        return batch.BatchDTO

    def get_latest(self) -> typing.Optional[batch.BatchDTO]:
        # noinspection PyTypeChecker
        return (
            self.session.query(batch.BatchDTO)
            .order_by(desc(batch.BatchDTO.ts))  # type: ignore
            .first()
        )
