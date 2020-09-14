import datetime
from typing import List, Protocol, runtime_checkable

import typing

from adapters import timestamp_adapter
from domain import batch_delta
from domain import batch, job_result, job_spec, value_objects
from services import (
    batch_logging_service,
    job_runner,
)
from services import job_logging_service, unit_of_work


@runtime_checkable
class BatchRunner(Protocol):
    def __call__(
        self,
        uow: unit_of_work.UnitOfWork,
        batch_id: value_objects.UniqueId,
        jobs: List[job_spec.JobSpec],
        batch_logger: batch_logging_service.BatchLoggingService,
        ts_adapter: timestamp_adapter.TimestampAdapter,
    ) -> batch.Batch:
        ...


def run(
    uow: unit_of_work.UnitOfWork,
    jobs: typing.Iterable[job_spec.JobSpec],
    ts_adapter: timestamp_adapter.TimestampAdapter,
) -> batch_delta.BatchDelta:
    batch_id = value_objects.UniqueId.generate()
    batch_logger = batch_logging_service.DefaultBatchLoggingService(
        uow=uow, batch_id=batch_id
    )

    try:
        with uow:
            previous_results = uow.batches.get_latest()
        batch_logger.log_info(
            value_objects.LogMessage(f"Staring batch {batch_id.value}...")
        )
        result = _run_batch(
            uow=uow, batch_id=batch_id, jobs=jobs, ts_adapter=ts_adapter
        )
        uow.batches.add(result)
        batch_logger.log_info(value_objects.LogMessage("Batch finished."))
        return batch_delta.BatchDelta(
            current_results=result,
            previous_results=previous_results,
        )
    except Exception as e:
        batch_logger.log_error(value_objects.LogMessage(str(e)))
        raise


def _run_batch(
    uow: unit_of_work.UnitOfWork,
    batch_id: value_objects.UniqueId,
    jobs: typing.Iterable[job_spec.JobSpec],
    ts_adapter: timestamp_adapter.TimestampAdapter,
) -> batch.Batch:
    job_results: List[job_result.JobResult] = []
    start_time = datetime.datetime.now()
    for job in jobs:
        job_id = value_objects.UniqueId.generate()
        job_logger = job_logging_service.DefaultJobLoggingService(
            uow=uow, batch_id=batch_id, job_id=job_id
        )
        result = job_runner.default_job_runner(
            uow=uow,
            job=job,
            logger=job_logger,
            batch_id=batch_id,
            job_id=job_id,
            ts_adapter=ts_adapter,
        )
        job_results.append(result)

    end_time = datetime.datetime.now()
    execution_millis = int((end_time - start_time).total_seconds() * 1000)
    return batch.Batch(
        id=batch_id,
        execution_millis=value_objects.ExecutionMillis(execution_millis),
        job_results=frozenset(job_results),
        execution_success_or_failure=value_objects.Result.success(),
        ts=uow.ts_adapter.now(),
    )
