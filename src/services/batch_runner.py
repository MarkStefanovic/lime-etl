import datetime
from typing import List, Optional, Protocol, runtime_checkable

from src.domain import batch, msg, job_result, job_spec, value_objects
from src.services import (
    batch_logging_service,
    job_logging_service,
    job_runner,
    unit_of_work,
)


@runtime_checkable
class BatchRunner(Protocol):
    def __call__(
        self,
        uow: unit_of_work.UnitOfWork,
        batch_id: value_objects.UniqueId,
        jobs: List[job_spec.JobSpec],
        batch_logger: batch_logging_service.BatchLoggingService,
    ) -> batch.Batch:
        ...


def run_and_email_results(
    uow: unit_of_work.UnitOfWork, jobs: List[job_spec.JobSpec],
) -> batch.Batch:
    with uow:
        previous_results = uow.batches.get_latest()
        current_results = run(uow=uow, jobs=jobs)
        if previous_results:
            m = _compose_email(
                previous_results=previous_results, current_results=current_results,
            )
            if m:
                uow.emailer.send(email=m)
        return current_results


def run(uow: unit_of_work.UnitOfWork, jobs: List[job_spec.JobSpec]) -> batch.Batch:
    with uow:
        batch_id = value_objects.UniqueId.generate()
        batch_logger = batch_logging_service.DefaultBatchLoggingService(
            batch_id=batch_id
        )

        try:
            batch_logger.log_start(uow=uow)
            result = _run_batch(uow=uow, batch_id=batch_id, jobs=jobs,)
            batch_logger.log_completed_successfully(uow=uow)
            return result
        except Exception as e:
            batch_logger.log_error(
                uow=uow, message=value_objects.LogMessage(value=str(e))
            )
            return batch.Batch(
                id=batch_id,
                job_results=frozenset(),
                execution_success_or_failure=value_objects.Result.failure(str(e)),
                execution_millis=value_objects.ExecutionMillis(0),
                ts=uow.ts_adapter.now(),
            )


def _compose_email(
    current_results: batch.Batch, previous_results: batch.Batch,
) -> Optional[msg.Msg]:
    common_jobs = previous_results.job_names & current_results.job_names
    fixed_jobs = common_jobs & (
        previous_results.broken_jobs - current_results.broken_jobs
    )
    newly_broken_jobs = current_results.broken_jobs - previous_results.broken_jobs
    subject = value_objects.EmailSubject(
        f"{len(fixed_jobs)} jobs fixed; {len(current_results.broken_jobs)} jobs broken"
    )
    broken_jobs_str = "The following jobs are broken:\n    " + "\n    ".join(
        f"{j}\n    " for j in current_results.broken_jobs
    )
    fixed_jobs_str = "The following jobs are fixed:\n    " + "\n    ".join(
        f"{j}\n    " for j in fixed_jobs
    )
    email_message = value_objects.EmailMsg(f"{broken_jobs_str}\n\n{fixed_jobs_str}")
    if newly_broken_jobs or fixed_jobs:
        return msg.Msg(subject=subject, message=email_message)
    else:
        return None


def _run_batch(
    uow: unit_of_work.UnitOfWork,
    batch_id: value_objects.UniqueId,
    jobs: List[job_spec.JobSpec],
) -> batch.Batch:
    job_results: List[job_result.JobResult] = []
    start_time = datetime.datetime.now()
    for job in jobs:
        job_id = value_objects.UniqueId.generate()
        job_logger = job_logging_service.DefaultJobLoggingService(
            batch_id=batch_id, job_id=job_id
        )
        result = job_runner.default_job_runner(
            uow=uow, job=job, logger=job_logger, batch_id=batch_id, job_id=job_id,
        )
        job_results.append(result)

    end_time = datetime.datetime.now()
    execution_millis = int((end_time - start_time).total_seconds() * 1000)
    return batch.Batch(
        id=batch_id,
        execution_millis=value_objects.ExecutionMillis(value=execution_millis),
        job_results=frozenset(job_results),
        execution_success_or_failure=value_objects.Result.success(),
        ts=uow.ts_adapter.now(),
    )
