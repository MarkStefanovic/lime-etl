from __future__ import annotations

import itertools
import typing
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from adapters import email_adapter, orm, timestamp_adapter
from domain import batch_delta, job_spec, value_objects
from services import batch_runner, unit_of_work
from services.admin import delete_old_logs


DEFAULT_ADMIN_JOBS = [
    delete_old_logs.DeleteOldLogs(days_to_keep=value_objects.DaysToKeep(3)),
]


def run(
    *,
    db_uri: str,
    etl_jobs: typing.Iterable[job_spec.JobSpec],
    admin_jobs: typing.Iterable[job_spec.JobSpec] = frozenset(DEFAULT_ADMIN_JOBS),
    schema: typing.Optional[str] = None,
    ts_adapter: timestamp_adapter.TimestampAdapter = timestamp_adapter.LocalTimestampAdapter(),
    email_adapter: typing.Optional[email_adapter.EmailAdapter] = None
) -> batch_delta.BatchDelta:
    if schema:
        orm.set_schema(schema=value_objects.SchemaName(schema))

    orm.start_mappers()
    engine = create_engine(db_uri, echo=True)
    orm.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    uow = unit_of_work.DefaultUnitOfWork(session_factory=session_factory)
    result: batch_delta.BatchDelta = batch_runner.run(
        uow=uow, jobs=itertools.chain(admin_jobs, etl_jobs), ts_adapter=ts_adapter
    )
    if email_adapter:
        email_adapter.send(result=result)
    return result

