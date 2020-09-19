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
        with uow:
            current_batch = uow.batches.get_batch_by_id(batch_id)

        if current_batch is None:
            raise Exception(
                "The current batch has not been saved to the database, so the results of dependent "
                "jobs cannot be checked."
            )
        else:
            dep_exceptions = {
                jr.job_name
                for jr in current_batch.job_results
                if jr.job_name in job.dependencies
                and jr.execution_success_or_failure.is_failure
            }
            dep_test_failures = {
                jr.job_name
                for jr in current_batch.job_results
                if jr.job_name in job.dependencies
                and jr.is_broken
            }
            if dep_exceptions and dep_test_failures:
                raise Exception(
                     f"The following dependencies failed to execute: {', '.join(sorted(dep_exceptions))} "
                     f"and the following jobs had test failures: {', '.join(sorted(dep_exceptions))}"
                )
            elif dep_exceptions:
                raise Exception(
                     f"The following dependencies failed to execute: {', '.join(sorted(dep_exceptions))}"
                )
            else:
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
        result: value_objects.Result = job.run(uow=uow, logger=logger)
    elif isinstance(job, job_spec.ETLJobSpec):
        result = job.run(logger=logger)
    else:
        raise ValueError(f"Expected an instance of AdminJobSpec or ETLJobSpec, but got {job}.")

    if result is None:
        result = value_objects.Result.success()
    elif isinstance(result, value_objects.Result):
        logger.log_info(value_objects.LogMessage(f"[{job.job_name}] finished successfully."))
    else:
        raise ValueError(
            f"A job should return an instance of either Success or Failure, but [{job.job_name}] "
            f"returned {result!r}."
        )

    if result.is_failure:
        logger.log_info(
            value_objects.LogMessage(
                f"[{job.job_name}] failed, so there is no point in running the tests.  "
                f"They have been skipped."
            )
        )
        test_results: typing.Collection[job_test_result.SimpleJobTestResult] = []
    else:
        logger.log_info(
            value_objects.LogMessage(f"Running the tests for [{job.job_name}]...")
        )
        if isinstance(job, job_spec.AdminJobSpec):
            test_results = job.test(logger=logger, uow=uow)
        else:
            test_results = job.test(logger=logger)

    if test_results:
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
    else:
        full_test_results = frozenset()

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
