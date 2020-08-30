import datetime
import traceback
from typing import Protocol

from src.domain import job_result, job_spec, value_objects
from src.services import job_logging_service, unit_of_work


class JobRunner(Protocol):
    def __call__(
        self,
        uow: unit_of_work.UnitOfWork,
        batch_id: value_objects.UniqueId,
        job_id: value_objects.UniqueId,
        job: job_spec.JobSpec,
        logger: job_logging_service.JobLoggingService,
    ) -> job_result.JobResult:
        ...


def default_job_runner(
    uow: unit_of_work.UnitOfWork,
    batch_id: value_objects.UniqueId,
    job_id: value_objects.UniqueId,
    job: job_spec.JobSpec,
    logger: job_logging_service.JobLoggingService,
) -> job_result.JobResult:
    with uow:
        try:
            logger.log_start(uow=uow)
            result = _run_with_retry(
                uow=uow,
                batch_id=batch_id,
                job_id=job_id,
                job=job,
                logger=logger,
                retries_so_far=0,
                max_retries=job.max_retries.value,
            )
            logger.log_completed_successfully(uow=uow)
            return result
        except Exception as e:
            logger.log_error(
                uow=uow,
                message=value_objects.LogMessage(
                    value=(
                        f"An error occurred while running the job {job.job_name.value}:"
                        f"\n{traceback.format_exc(10)}"
                    )
                ),
            )
            return job_result.JobResult(
                id=job_id,
                batch_id=batch_id,
                job_name=job.job_name,
                test_results=frozenset(),
                execution_success_or_failure=value_objects.Result.failure(
                    message=str(e)
                ),
                execution_millis=value_objects.ExecutionMillis(value=0),
                ts=uow.ts_adapter.now(),
            )


def _run_with_retry(
    uow: unit_of_work.UnitOfWork,
    batch_id: value_objects.UniqueId,
    job_id: value_objects.UniqueId,
    job: job_spec.JobSpec,
    max_retries: int,
    retries_so_far: int,
    logger: job_logging_service.JobLoggingService,
) -> job_result.JobResult:
    # noinspection PyBroadException
    try:
        return _run_job_with_tests(
            uow=uow, batch_id=batch_id, job_id=job_id, job=job, logger=logger,
        )
    except:
        if max_retries > retries_so_far:
            logger.log_retry(
                uow=uow, retry=retries_so_far, max_retries=max_retries,
            )
            return _run_with_retry(
                uow=uow,
                batch_id=batch_id,
                job_id=job_id,
                job=job,
                max_retries=max_retries,
                retries_so_far=retries_so_far + 1,
                logger=logger,
            )
        else:
            raise


def _run_job_with_tests(
    uow: unit_of_work.UnitOfWork,
    batch_id: value_objects.UniqueId,
    job_id: value_objects.UniqueId,
    job: job_spec.JobSpec,
    logger: job_logging_service.JobLoggingService,
) -> job_result.JobResult:
    start_time = datetime.datetime.now()
    job.run(logger=logger,)
    test_results = job.test(logger=logger,)
    test_results = frozenset(test_results)
    end_time = datetime.datetime.now()
    execution_millis = int((end_time - start_time).total_seconds() * 1000)
    ts = uow.ts_adapter.now()
    return job_result.JobResult(
        id=job_id,
        batch_id=batch_id,
        job_name=job.job_name,
        test_results=test_results,
        execution_millis=value_objects.ExecutionMillis(execution_millis),
        execution_success_or_failure=value_objects.Result.success(),
        ts=ts,
    )
