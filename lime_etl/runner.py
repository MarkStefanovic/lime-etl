from __future__ import annotations

import typing

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from lime_etl.adapters import orm, timestamp_adapter
from lime_etl.domain import batch_delta, job_spec, shared_resource, value_objects
from lime_etl.services import admin_unit_of_work, batch_logging_service, batch_runner
from lime_etl.services.admin import delete_old_logs


def run_admin(
    *,
    engine_or_uri: typing.Union[sa.engine.Engine, str],
    schema: typing.Optional[str] = "etl",
    skip_tests: bool = False,
    days_logs_to_keep: int = 3,
) -> batch_delta.BatchDelta:
    days_to_keep = value_objects.DaysToKeep(days_logs_to_keep)
    if schema:
        orm.set_schema(schema=value_objects.SchemaName(schema))

    orm.start_mappers()
    if type(engine_or_uri) is sa.engine.Engine:
        engine = typing.cast(sa.engine.Connectable, engine_or_uri)
    else:
        engine = sa.create_engine(engine_or_uri)
    orm.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    batch_id = value_objects.UniqueId.generate()
    ts_adapter = timestamp_adapter.LocalTimestampAdapter()
    logger = batch_logging_service.BatchLoggingService(
        batch_id=batch_id,
        session=session_factory(),
        ts_adapter=ts_adapter,
    )
    uow = admin_unit_of_work.SqlAlchemyAdminUnitOfWork(
        session_factory=session_factory,
        ts_adapter=ts_adapter,
    )
    result: batch_delta.BatchDelta = batch_runner.run(
        batch_name=value_objects.BatchName("admin"),
        admin_uow=uow,
        batch_id=batch_id,
        jobs=[
            delete_old_logs.DeleteOldLogs(days_to_keep=days_to_keep),
        ],
        logger=logger,
        resources=[],
        skip_tests=skip_tests,
        ts_adapter=ts_adapter,
    )
    return result


def run(
    *,
    batch_name: str,
    engine_or_uri: typing.Union[sa.engine.Engine, str],
    jobs: typing.Iterable[job_spec.JobSpec],
    schema: typing.Optional[str] = None,
    resources: typing.Collection[
        shared_resource.SharedResource[typing.Any]
    ] = frozenset(),
    skip_tests: bool = False,
) -> batch_delta.BatchDelta:
    name = value_objects.BatchName(batch_name)
    if schema:
        orm.set_schema(schema=value_objects.SchemaName(schema))

    orm.start_mappers()
    if type(engine_or_uri) is sa.engine.Engine:
        engine = typing.cast(sa.engine.Connectable, engine_or_uri)
    else:
        engine = sa.create_engine(engine_or_uri)
    orm.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    ts_adapter = timestamp_adapter.LocalTimestampAdapter()
    admin_uow = admin_unit_of_work.SqlAlchemyAdminUnitOfWork(
        session_factory=session_factory,
        ts_adapter=ts_adapter,
    )
    batch_id = value_objects.UniqueId.generate()
    logger = batch_logging_service.BatchLoggingService(
        batch_id=batch_id,
        session=session_factory(),
        ts_adapter=ts_adapter,
    )
    return batch_runner.run(
        batch_name=name,
        admin_uow=admin_uow,
        batch_id=batch_id,
        jobs=list(jobs),
        logger=logger,
        resources=resources,
        skip_tests=skip_tests,
        ts_adapter=ts_adapter,
    )
