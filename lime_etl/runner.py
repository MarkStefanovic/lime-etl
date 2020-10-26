from __future__ import annotations

import multiprocessing
import typing

import lime_uow as lu
import sqlalchemy as sa

from lime_etl.adapters import admin_session
from lime_etl.domain import batch_result, batch_spec, value_objects
from lime_etl.services import admin_unit_of_work, batch_logging_service, batch_runner
from lime_etl.services.admin import admin_batch


def run_batch(
    batch: batch_spec.BatchSpec[typing.Any],
    admin_engine_or_uri: typing.Union[sa.engine.Engine, str],
    admin_schema: typing.Optional[str] = "etl",
) -> batch_result.BatchResult:
    session_factory = admin_session.admin_session_factory(
        engine_or_uri=admin_engine_or_uri,
        schema=admin_schema,
    )
    logger = batch_logging_service.BatchLoggingService(
        batch_id=batch.batch_id,
        session=session_factory(),
        ts_adapter=batch.ts_adapter,
    )
    with lu.SharedResources(
        admin_session.SqlAlchemyAdminSession(session_factory)
    ) as shared_resources:
        admin_uow = admin_unit_of_work.SqlAlchemyAdminUnitOfWork(
            shared_resources=shared_resources,
            ts_adapter=batch.ts_adapter,
        )
        return batch_runner.run(
            admin_uow=admin_uow,
            batch_name=batch.batch_name,
            batch_id=batch.batch_id,
            jobs=batch.job_specs,
            logger=logger,
            skip_tests=batch.skip_tests.value,
            ts_adapter=batch.ts_adapter,
        )


def run_admin(
    *,
    admin_engine_or_uri: typing.Union[sa.engine.Engine, str],
    schema: typing.Optional[str] = "etl",
    skip_tests: bool = False,
    days_logs_to_keep: int = 3,
) -> batch_result.BatchResult:
    if type(admin_engine_or_uri) is sa.engine.Engine:
        engine: typing.Optional[sa.engine.Engine] = typing.cast(sa.engine.Engine, admin_engine_or_uri)
        db_uri = value_objects.DbUri(str(typing.cast(sa.engine.Engine, engine).url))
    else:
        engine = None
        db_uri = value_objects.DbUri(typing.cast(str, admin_engine_or_uri))
    admin_schema = value_objects.SchemaName(schema)
    days_to_keep = value_objects.DaysToKeep(days_logs_to_keep)
    skip_tests_flag = value_objects.Flag(skip_tests)
    batch = admin_batch.AdminBatch(
        admin_db_uri=db_uri,
        admin_schema=admin_schema,
        days_logs_to_keep=days_to_keep,
        skip_tests=skip_tests_flag,
    )
    if engine:
        engine_or_uri: typing.Union[sa.engine.Engine, str] = engine
    else:
        engine_or_uri = db_uri.value
    return run_batch(
        admin_engine_or_uri=engine_or_uri,
        admin_schema=admin_schema.value,
        batch=batch,
    )


def run_batches_in_parallel(
    admin_db_uri: str,
    batches: typing.Iterable[batch_spec.BatchSpec[typing.Any]],
    max_processes: int = 3,
    schema: typing.Optional[str] = "etl",
    timeout: typing.Optional[int] = None,
) -> typing.List[batch_result.BatchResult]:
    params = [(batch, admin_db_uri, schema) for batch in batches]
    with multiprocessing.Pool(max_processes) as pool:
        future = pool.starmap_async(run_batch, params)
        return future.get(timeout)
