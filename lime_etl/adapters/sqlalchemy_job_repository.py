import typing

from lime_uow import sqlalchemy_resources as lsa
from sqlalchemy import orm
from sqlalchemy.sql.expression import desc

from lime_etl import domain


__all__ = ("SqlAlchemyJobRepository",)


class SqlAlchemyJobRepository(
    domain.JobRepository, lsa.SqlAlchemyRepository[domain.JobResultDTO]
):
    def __init__(
        self,
        session: orm.Session,
        ts_adapter: domain.TimestampAdapter,
    ):
        self._ts_adapter = ts_adapter
        super().__init__(session)

    @property
    def entity_type(self) -> typing.Type[domain.JobResultDTO]:
        return domain.JobResultDTO

    def get_latest(
        self, job_name: domain.JobName, /
    ) -> typing.Optional[domain.JobResultDTO]:
        # noinspection PyTypeChecker
        return (
            self.session.query(domain.JobResultDTO)
            .order_by(desc(domain.JobResultDTO.ts))  # type: ignore
            .first()
        )

    def get_last_successful_ts(
        self, job_name: domain.JobName, /
    ) -> typing.Optional[domain.Timestamp]:
        # noinspection PyUnresolvedReferences,PyTypeChecker
        jr: typing.Optional[domain.JobResultDTO] = (
            self._session.query(domain.JobResultDTO)
            .filter(domain.JobResultDTO.job_name.ilike(job_name.value))  # type: ignore
            .filter(domain.JobResultDTO.execution_error_occurred.is_(False))  # type: ignore
            .order_by(desc(domain.JobResultDTO.ts))  # type: ignore
            .first()
        )
        if jr is None:
            return None
        else:
            return domain.Timestamp(jr.ts)

    @classmethod
    def interface(cls) -> typing.Type[domain.JobRepository]:
        return domain.JobRepository
