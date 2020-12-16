import datetime
import typing

import sqlalchemy as sa
from lime_uow import sqlalchemy_resources as lsa
from sqlalchemy.orm.session import Session

from lime_etl import domain

__all__ = ("SqlAlchemyBatchRepository",)


class SqlAlchemyBatchRepository(
    domain.BatchRepository, lsa.SqlAlchemyRepository[domain.BatchStatusDTO]
):
    def __init__(
        self,
        session: Session,
        ts_adapter: domain.TimestampAdapter,
    ):
        super().__init__(session)
        self._ts_adapter = ts_adapter

    def delete_old_entries(self, days_to_keep: domain.DaysToKeep, /) -> int:
        ts = self._ts_adapter.now().value
        cutoff: datetime.datetime = ts - datetime.timedelta(days=days_to_keep.value)
        # We need to delete batches one by one to trigger cascade deletes, a bulk update will
        # not trigger them, and we don't want to rely on specific database implementations, so
        # we cannot use ondelete='CASCADE' on the foreign key columns.
        batches: typing.List[domain.BatchStatusDTO] = (
            self.session.query(domain.BatchStatusDTO)
            .filter(domain.BatchStatusDTO.ts < cutoff)
            .all()
        )
        for b in batches:
            self.session.delete(b)
        return len(batches)

    @property
    def entity_type(self) -> typing.Type[domain.BatchStatusDTO]:
        return domain.BatchStatusDTO

    def get_latest(self) -> typing.Optional[domain.BatchStatusDTO]:
        # noinspection PyTypeChecker
        return (
            self.session.query(domain.BatchStatusDTO)
            .order_by(sa.desc(domain.BatchStatusDTO.ts))  # type: ignore
            .first()
        )

    def get_previous(self) -> typing.Optional[domain.BatchStatusDTO]:
        # noinspection PyTypeChecker
        return (
            self.session.query(domain.BatchStatusDTO)
                .order_by(sa.desc(domain.BatchStatusDTO.ts))  # type: ignore
                .offset(1)
                .first()
        )

    @classmethod
    def interface(cls) -> typing.Type[domain.BatchRepository]:
        return domain.BatchRepository
