import typing

from lime_uow import sqlalchemy_resources as lsa
from sqlalchemy import orm
from sqlalchemy.sql.expression import desc

from lime_etl import domain

__all__ = ("SqlAlchemyJobRepository",)

from lime_etl.domain import job_status, value_objects


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

    def last_job_run_status(
        self, /, job_name: value_objects.JobName,
    ) -> typing.Optional[job_status.JobStatus]:
        # noinspection PyUnresolvedReferences,PyTypeChecker
        jr: typing.Optional[domain.JobResultDTO] = (
            self._session.query(domain.JobResultDTO)
            .filter(domain.JobResultDTO.job_name.ilike(job_name.value))  # type: ignore
            .filter(domain.JobResultDTO.skipped.is_(False))  # type: ignore
            .order_by(desc(domain.JobResultDTO.ts))  # type: ignore
            .offset(1)
            .first()
        )
        if jr is None:
            return None
        else:
            return jr.to_domain().status

    def latest_test_results(
        self, /, job_name: value_objects.JobName,
    ) -> typing.FrozenSet[domain.JobTestResult]:
        # noinspection PyUnresolvedReferences,PyTypeChecker
        jr: typing.Optional[domain.JobResultDTO] = (
            self._session.query(domain.JobResultDTO)
            .filter(domain.JobResultDTO.job_name.ilike(job_name.value))  # type: ignore
            .filter(domain.JobResultDTO.skipped.is_(False))  # type: ignore
            .order_by(desc(domain.JobResultDTO.ts))  # type: ignore
            .offset(1)
            .first()
        )
        if jr is None:
            return frozenset()
        else:
            return jr.to_domain().test_results

    @property
    def entity_type(self) -> typing.Type[domain.JobResultDTO]:
        return domain.JobResultDTO

    def get_last_successful_ts(
        self, job_name: domain.JobName, /
    ) -> typing.Optional[domain.Timestamp]:
        # noinspection PyUnresolvedReferences,PyTypeChecker
        jr: typing.Optional[domain.JobResultDTO] = (
            self._session.query(domain.JobResultDTO)
            .filter(domain.JobResultDTO.job_name.ilike(job_name.value))  # type: ignore
            .filter(domain.JobResultDTO.execution_error_occurred.is_(False))  # type: ignore
            .filter(domain.JobResultDTO.skipped.is_(False))  # type: ignore
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
