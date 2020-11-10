from __future__ import annotations

import multiprocessing
import typing

import lime_uow as lu
import sqlalchemy as sa
from sqlalchemy import orm

from lime_etl import domain, adapters, services
from lime_etl.adapters import admin_orm


def run_batch(
    batch: services.BatchSpec[typing.Any],
    admin_engine_or_uri: typing.Union[sa.engine.Engine, str],
    admin_schema: typing.Optional[str] = "etl",
) -> domain.BatchResult:
    session_factory = adapters.admin_session_factory(
        engine_or_uri=admin_engine_or_uri,
        schema=admin_schema,
    )
    logger = services.BatchLoggingService(
        batch_id=batch.batch_id,
        session=session_factory(),
        ts_adapter=batch.ts_adapter,
    )
    admin_uow = services.SqlAlchemyAdminUnitOfWork(
        session_factory=session_factory, ts_adapter=batch.ts_adapter
    )
    return services.run(
        admin_uow=admin_uow,
        batch_name=batch.batch_name,
        batch_id=batch.batch_id,
        batch_uow=batch.uow,
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
) -> domain.BatchResult:
    if type(admin_engine_or_uri) is sa.engine.Engine:
        engine: typing.Optional[sa.engine.Engine] = typing.cast(
            sa.engine.Engine, admin_engine_or_uri
        )
        db_uri = domain.DbUri(str(typing.cast(sa.engine.Engine, engine).url))
    else:
        engine = sa.create_engine(admin_engine_or_uri)
        admin_orm.metadata.create_all(bind=engine)
        db_uri = domain.DbUri(typing.cast(str, admin_engine_or_uri))

    admin_schema = domain.SchemaName(schema)
    days_to_keep = domain.DaysToKeep(days_logs_to_keep)
    skip_tests_flag = domain.Flag(skip_tests)
    session_factory = orm.sessionmaker(bind=engine)
    batch = services.AdminBatch(
        session_factory=session_factory,
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
    batches: typing.Iterable[services.BatchSpec[typing.Any]],
    max_processes: int = 3,
    schema: typing.Optional[str] = "etl",
    timeout: typing.Optional[int] = None,
) -> typing.List[domain.BatchResult]:
    params = [(batch, admin_db_uri, schema) for batch in batches]
    with multiprocessing.Pool(max_processes) as pool:
        future = pool.starmap_async(run_batch, params)
        return future.get(timeout)
