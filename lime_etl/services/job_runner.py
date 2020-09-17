import datetime
import traceback

import typing

from adapters import timestamp_adapter  # type: ignore
from domain import job_result, job_spec, job_test_result, value_objects  # type: ignore
from services import job_logging_service, unit_of_work  # type: ignore


class JobRunner(typing.Protocol):
    def __call__(
        self,
        uow: unit_of_work.UnitOfWork,
        batch_id: value_objects.UniqueId,
        job_id: value_objects.UniqueId,
        job: job_spec.JobSpec,
        logger: job_logging_service.JobLoggingService,
        ts_adapter: timestamp_adapter.TimestampAdapter,
    ) -> job_result.JobResult:
        ...


def default_job_runner(
    uow: unit_of_work.UnitOfWork,
    batch_id: value_objects.UniqueId,
    job_id: value_objects.UniqueId,
    job: job_spec.JobSpec,
    logger: job_logging_service.JobLoggingService,
    ts_adapter: timestamp_adapter.TimestampAdapter,
) -> job_result.JobResult:
    try:
        logger.log_info(value_objects.LogMessage(f"Starting [{job.job_name.value}]..."))
        result = _run_with_retry(
            batch_id=batch_id,
            job_id=job_id,
            job=job,
            logger=logger,
            retries_so_far=0,
            max_retries=job.max_retries.value,
            ts_adapter=ts_adapter,
            uow=uow,
        )
        logger.log_info(
            value_objects.LogMessage(f"Finished running [{job.job_name.value}].")
        )
        return result
    except Exception as e:
        logger.log_error(
            value_objects.LogMessage(
                (
                    f"An error occurred while running the job [{job.job_name.value}]:"
                    f"\n{traceback.format_exc(10)}"
                )
            ),
        )
        return job_result.JobResult(
            id=job_id,
            batch_id=batch_id,
            job_name=job.job_name,
            test_results=frozenset(),
            execution_success_or_failure=value_objects.Result.failure(str(e)),
            execution_millis=value_objects.ExecutionMillis(0),
            ts=ts_adapter.now(),
        )


def _run_with_retry(
    batch_id: value_objects.UniqueId,
    job_id: value_objects.UniqueId,
    job: job_spec.JobSpec,
    max_retries: int,
    retries_so_far: int,
    logger: job_logging_service.JobLoggingService,
    ts_adapter: timestamp_adapter.TimestampAdapter,
    uow: unit_of_work.UnitOfWork,
) -> job_result.JobResult:
    # noinspection PyBroadException
    try:
        return _run_job_with_tests(
            batch_id=batch_id,
            job_id=job_id,
            job=job,
            logger=logger,
            ts_adapter=ts_adapter,
            uow=uow,
        )
    except:
        if max_retries > retries_so_far:
            logger.log_info(
                value_objects.LogMessage(
                    f"Running retry {retries_so_far} or {max_retries}..."
                )
            )
            return _run_with_retry(
                batch_id=batch_id,
                job_id=job_id,
                job=job,
                max_retries=max_retries,
                retries_so_far=retries_so_far + 1,
                logger=logger,
                ts_adapter=ts_adapter,
                uow=uow,
            )
        else:
            logger.log_info(
                value_objects.LogMessage(
                    f"[{job.job_name.value}] failed after {max_retries} retries."
                )
            )
            raise


def _run_job_with_tests(
    batch_id: value_objects.UniqueId,
    job_id: value_objects.UniqueId,
    job: job_spec.JobSpec,
    logger: job_logging_service.JobLoggingService,
    ts_adapter: timestamp_adapter.TimestampAdapter,
    uow: unit_of_work.UnitOfWork,
) -> job_result.JobResult:
    start_time = datetime.datetime.now()
    if isinstance(job, job_spec.AdminJobSpec):
        result = job.run(uow=uow, logger=logger)
        test_results: typing.Collection[job_test_result.SimpleJobTestResult] = job.test(
            logger=logger, uow=uow
        )
    elif isinstance(job, job_spec.ETLJobSpec):
        result = job.run(logger=logger)
        test_results: typing.Collection[job_test_result.SimpleJobTestResult] = job.test(
            logger=logger
        )
    else:
        raise ValueError(
            f"Expected an instance of AdminJobSpec or ETLJobSpec, but got {job}."
        )

    full_test_results = frozenset(
        job_test_result.JobTestResult(
            id=value_objects.UniqueId.generate(),
            job_id=job_id,
            test_name=test_result.test_name,
            test_success_or_failure=test_result.test_success_or_failure,
            ts=ts_adapter.now(),
        )
        for test_result in test_results
    )
    end_time = datetime.datetime.now()
    execution_millis = int((end_time - start_time).total_seconds() * 1000)
    ts = ts_adapter.now()
    return job_result.JobResult(
        id=job_id,
        batch_id=batch_id,
        job_name=job.job_name,
        test_results=full_test_results,
        execution_millis=value_objects.ExecutionMillis(execution_millis),
        execution_success_or_failure=value_objects.Result.success(),
        ts=ts,
    )
