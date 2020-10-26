import typing

import lime_uow as lu
from sqlalchemy import orm

from lime_etl.adapters import admin_session, timestamp_adapter
from lime_etl.domain import batch_spec, job_spec, value_objects
from lime_etl.services import admin_unit_of_work
from lime_etl.services.admin import delete_old_logs


class AdminBatch(batch_spec.BatchSpec[admin_unit_of_work.AdminUnitOfWork]):
    def __init__(
        self,
        admin_db_uri: value_objects.DbUri,
        admin_schema: value_objects.SchemaName,
        batch_id: typing.Optional[value_objects.UniqueId] = None,
        days_logs_to_keep: value_objects.DaysToKeep = value_objects.DaysToKeep(3),
        skip_tests: value_objects.Flag = value_objects.Flag(False),
        timeout_seconds: typing.Optional[value_objects.TimeoutSeconds] = None,
        ts_adapter: timestamp_adapter.TimestampAdapter = timestamp_adapter.LocalTimestampAdapter(),
    ):
        self._admin_db_uri = admin_db_uri
        self._admin_schema = admin_schema
        self._batch_id = batch_id
        self._days_logs_to_keep = days_logs_to_keep
        self._session_factory: typing.Optional[orm.sessionmaker] = None

        super().__init__(
            batch_name=value_objects.BatchName("admin"),
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
        session_factory = admin_session.admin_session_factory(
            engine_or_uri=self._admin_db_uri.value,
            schema=self._admin_schema.value,
        )
        return lu.SharedResources(
            admin_session.SqlAlchemyAdminSession(session_factory),
        )

    def create_uow(
        self, shared_resources: lu.SharedResources
    ) -> admin_unit_of_work.AdminUnitOfWork:
        return admin_unit_of_work.SqlAlchemyAdminUnitOfWork(
            shared_resources=shared_resources,
            ts_adapter=self.ts_adapter,
        )
