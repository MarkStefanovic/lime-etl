import typing

import sqlalchemy as sa
from sqlalchemy import orm

from lime_etl import domain, adapter
from lime_etl.service import admin, batch_runner

__all__ = ("AdminBatch",)


class AdminBatch(domain.BatchSpec[domain.admin_unit_of_work.AdminUnitOfWork]):
    def __init__(
        self,
        *,
        admin_engine_uri: domain.DbUri,
        admin_schema: domain.SchemaName = domain.SchemaName("etl"),
        days_logs_to_keep: domain.DaysToKeep = domain.DaysToKeep(3),
        min_seconds_between_runs: domain.MinSecondsBetweenRefreshes = domain.MinSecondsBetweenRefreshes(12 * 60 * 60),
        ts_adapter: domain.TimestampAdapter = adapter.LocalTimestampAdapter(),
    ):
        self._admin_engine_uri = admin_engine_uri
        self._admin_schema = admin_schema
        self._days_logs_to_keep = days_logs_to_keep
        self._min_seconds_between_runs = min_seconds_between_runs
        self._ts_adapter = ts_adapter

    @property
    def batch_name(self) -> domain.BatchName:
        return domain.BatchName("admin")

    def create_jobs(
        self, uow: domain.admin_unit_of_work.AdminUnitOfWork
    ) -> typing.List[domain.JobSpec[domain.admin_unit_of_work.AdminUnitOfWork]]:
        return [
            admin.delete_old_logs.DeleteOldLogs(
                days_logs_to_keep=self._days_logs_to_keep,
                min_seconds_between_runs=self._min_seconds_between_runs,
            ),
        ]

    def create_uow(self) -> domain.admin_unit_of_work.AdminUnitOfWork:
        admin_engine = sa.create_engine(self._admin_engine_uri.value)
        adapter.admin_metadata.create_all(bind=admin_engine)
        adapter.admin_orm.set_schema(schema=self._admin_schema)
        adapter.admin_orm.start_mappers()
        admin_session_factory = orm.sessionmaker(bind=admin_engine)
        return adapter.SqlAlchemyAdminUnitOfWork(
            session_factory=admin_session_factory, ts_adapter=self._ts_adapter
        )

    def run(self) -> domain.BatchStatus:
        return batch_runner.run_batch(
            batch=self,
            admin_engine_uri=self._admin_engine_uri,
            admin_schema=self._admin_schema,
            ts_adapter=self._ts_adapter,
        )
