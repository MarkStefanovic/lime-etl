import typing

import lime_uow as lu
from sqlalchemy import orm

from lime_etl import domain, adapters
from lime_etl.services import admin_unit_of_work, batch_spec, job_spec
from lime_etl.services.admin import delete_old_logs

__all__ = ("AdminBatch",)


class AdminBatch(batch_spec.BatchSpec[admin_unit_of_work.AdminUnitOfWork]):
    def __init__(
        self,
        session_factory: orm.sessionmaker,
        admin_schema: domain.SchemaName,
        batch_id: typing.Optional[domain.UniqueId] = None,
        days_logs_to_keep: domain.DaysToKeep = domain.DaysToKeep(3),
        skip_tests: domain.Flag = domain.Flag(False),
        timeout_seconds: domain.TimeoutSeconds = domain.TimeoutSeconds(None),
        ts_adapter: adapters.TimestampAdapter = adapters.LocalTimestampAdapter(),
    ):
        self._admin_schema = admin_schema
        self._batch_id = batch_id
        self._days_logs_to_keep = days_logs_to_keep
        self._session_factory = session_factory

        super().__init__(
            batch_name=domain.BatchName("admin"),
            batch_id=batch_id,
            skip_tests=skip_tests,
            timeout_seconds=timeout_seconds,
            ts_adapter=ts_adapter,
        )

    def create_job_specs(
        self, uow: admin_unit_of_work.AdminUnitOfWork
    ) -> typing.Tuple[job_spec.JobSpec, ...]:
        return (
            delete_old_logs.DeleteOldLogs(
                admin_uow=uow,
                days_to_keep=self._days_logs_to_keep,
            ),
        )

    def create_shared_resource(self) -> lu.SharedResources:
        return lu.SharedResources(
            adapters.SqlAlchemyAdminSession(self._session_factory),
        )

    def create_uow(self) -> admin_unit_of_work.AdminUnitOfWork:
        return admin_unit_of_work.SqlAlchemyAdminUnitOfWork(
            session_factory=self._session_factory, ts_adapter=self.ts_adapter
        )
