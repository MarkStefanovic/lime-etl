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

    @staticmethod
    def key() -> str:
        return domain.BatchRepository.__name__

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

    def get_latest(
        self, /, batch_name: domain.BatchName
    ) -> typing.Optional[domain.BatchStatusDTO]:
        # noinspection PyTypeChecker
        return (
            self.session.query(domain.BatchStatusDTO)
            .filter(domain.BatchStatusDTO.name == batch_name.value)
            .order_by(sa.desc(domain.BatchStatusDTO.ts))  # type: ignore
            .first()
        )

    def get_latest_batch_delta(
        self, /, batch_name: domain.BatchName
    ) -> domain.BatchDelta:
        if (current_dto := self.get_latest(batch_name)) is None:
            raise domain.exceptions.BatchNotFound(
                f"No previous results for batch {batch_name.value!r} were found."
            )

        if (previous_dto := self.get_previous(batch_name)) is None:
            previous = None
        else:
            previous = previous_dto.to_domain()

        return domain.BatchDelta(
            current_results=current_dto.to_domain(),
            previous_results=previous,
        )

    def get_previous(
        self, /, batch_name: domain.BatchName
    ) -> typing.Optional[domain.BatchStatusDTO]:
        # noinspection PyTypeChecker
        return (
            self.session.query(domain.BatchStatusDTO)
            .filter(domain.BatchStatusDTO.name == batch_name.value)
            .order_by(sa.desc(domain.BatchStatusDTO.ts))  # type: ignore
            .offset(1)
            .first()
        )
