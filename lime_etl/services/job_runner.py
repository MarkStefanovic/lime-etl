import datetime
import traceback
import typing

import lime_uow as lu

from lime_etl.adapters import timestamp_adapter
from lime_etl.domain import (
    exceptions,
    job_result,
    job_spec,
    job_test_result,
    value_objects,
)
from lime_etl.services import admin_unit_of_work, job_logging_service


class JobRunner(typing.Protocol):
    def __call__(
        self,
        *,
        admin_uow: admin_unit_of_work.AdminUnitOfWork,
        batch_id: value_objects.UniqueId,
        batch_uow: lu.UnitOfWork,
        job: job_spec.JobSpec,
        job_id: value_objects.UniqueId,
        logger: job_logging_service.AbstractJobLoggingService,
        skip_tests: bool,
        ts_adapter: timestamp_adapter.TimestampAdapter,
    ) -> job_result.JobResult:
        ...


def default_job_runner(
    *,
    admin_uow: admin_unit_of_work.AdminUnitOfWork,
    batch_id: value_objects.UniqueId,
    batch_uow: lu.UnitOfWork,
    job: job_spec.JobSpec,
    job_id: value_objects.UniqueId,
    logger: job_logging_service.AbstractJobLoggingService,
    skip_tests: bool,
    ts_adapter: timestamp_adapter.TimestampAdapter,
) -> job_result.JobResult:
    result = _run_job_pre_handlers(
        admin_uow=admin_uow,
        batch_id=batch_id,
        job=job,
        job_id=job_id,
        batch_uow=batch_uow,
        logger=logger,
        skip_tests=skip_tests,
        ts_adapter=ts_adapter,
    )
    assert result.execution_success_or_failure is not None
    if result.execution_success_or_failure.is_failure:
        new_job = job.on_execution_error(
            result.execution_success_or_failure.failure_message
        )
        if new_job:
            return default_job_runner(
                admin_uow=admin_uow,
                batch_id=batch_id,
                job=new_job,
                job_id=job_id,
                batch_uow=batch_uow,
                logger=logger,
                skip_tests=skip_tests,
                ts_adapter=ts_adapter,
            )
        else:
            return result
    elif any(test.test_failed for test in result.test_results):
        new_job = job.on_test_failure(result.test_results)
        if new_job:
            return default_job_runner(
                admin_uow=admin_uow,
                batch_id=batch_id,
                job=new_job,
                job_id=job_id,
                batch_uow=batch_uow,
                logger=logger,
                skip_tests=skip_tests,
                ts_adapter=ts_adapter,
            )
        else:
            return result
    else:
        return result


def _run_job_pre_handlers(
    *,
    admin_uow: admin_unit_of_work.AdminUnitOfWork,
    batch_id: value_objects.UniqueId,
    batch_uow: lu.UnitOfWork,
    job: job_spec.JobSpec,
    job_id: value_objects.UniqueId,
    logger: job_logging_service.AbstractJobLoggingService,
    skip_tests: bool,
    ts_adapter: timestamp_adapter.TimestampAdapter,
) -> job_result.JobResult:
    logger.log_info(f"Starting [{job.job_name.value}]...")
    start_time = ts_adapter.now()
    with admin_uow as uow:
        current_batch = uow.batch_repo.get(batch_id.value).to_domain()

    if current_batch is None:
        raise exceptions.BatchNotFound(batch_id)
    else:
        dep_exceptions = {
            jr.job_name.value
            for jr in current_batch.job_results
            if jr.job_name in job.dependencies
            and jr.execution_success_or_failure is not None
            and jr.execution_success_or_failure.is_failure
        }
        dep_test_failures = {
            jr.job_name.value
            for jr in current_batch.job_results
            if jr.job_name in job.dependencies and jr.tests_failed
        }
        if dep_exceptions and dep_test_failures:
            errs = ", ".join(sorted(dep_exceptions))
            test_failures = ", ".join(sorted(dep_test_failures))
            raise Exception(
                f"The following dependencies failed to execute: {errs} "
                f"and the following jobs had test failures: {test_failures}"
            )
        elif dep_exceptions:
            errs = ", ".join(sorted(dep_exceptions))
            raise Exception(f"The following dependencies failed to execute: {errs}")
        else:
            result = _run_jobs_with_tests(
                batch_id=batch_id,
                job=job,
                job_id=job_id,
                logger=logger,
                skip_tests=skip_tests,
                start_time=start_time,
                ts_adapter=ts_adapter,
                batch_uow=batch_uow,
            )
            logger.log_info(f"Finished running [{job.job_name.value}].")
            return result


def _run_jobs_with_tests(
    *,
    batch_id: value_objects.UniqueId,
    batch_uow: lu.UnitOfWork,
    job: job_spec.JobSpec,
    job_id: value_objects.UniqueId,
    logger: job_logging_service.AbstractJobLoggingService,
    skip_tests: bool,
    start_time: value_objects.Timestamp,
    ts_adapter: timestamp_adapter.TimestampAdapter,
) -> job_result.JobResult:
    result, execution_millis = _run_with_retry(
        job=job,
        logger=logger,
        max_retries=job.max_retries.value,
        retries_so_far=0,
        start_time=start_time,
        ts_adapter=ts_adapter,
        batch_uow=batch_uow,
    )
    if result.is_success:
        logger.log_info(f"[{job.job_name.value}] finished successfully.")
        if skip_tests:
            full_test_results: typing.FrozenSet[
                job_test_result.JobTestResult
            ] = frozenset()
        else:
            logger.log_info(f"Running the tests for [{job.job_name.value}]...")
            test_start_time = datetime.datetime.now()
            test_results = job.test(batch_uow, logger)
            test_execution_millis = int(
                (datetime.datetime.now() - test_start_time).total_seconds() * 1000
            )

            if test_results:
                tests_passed = sum(
                    1 for test_result in test_results if test_result.test_passed
                )
                tests_failed = sum(
                    1 for test_result in test_results if test_result.test_failed
                )
                logger.log_info(
                    f"{job.job_name.value} test results: {tests_passed=}, {tests_failed=}"
                )
                full_test_results = frozenset(
                    job_test_result.JobTestResult(
                        id=value_objects.UniqueId.generate(),
                        job_id=job_id,
                        test_name=test_result.test_name,
                        test_success_or_failure=test_result.test_success_or_failure,
                        execution_millis=value_objects.ExecutionMillis(
                            test_execution_millis
                        ),
                        execution_success_or_failure=value_objects.Result.success(),
                        ts=start_time,
                    )
                    for test_result in test_results
                )
            else:
                logger.log_info("The job test method returned no results.")
                full_test_results = frozenset()
    else:
        logger.log_info(
            f"An exception occurred while running [{job.job_name.value}]: "
            f"{result.failure_message}."
        )
        full_test_results = frozenset()

    return job_result.JobResult(
        id=job_id,
        batch_id=batch_id,
        job_name=job.job_name,
        test_results=full_test_results,
        execution_millis=execution_millis,
        execution_success_or_failure=value_objects.Result.success(),
        running=value_objects.Flag(False),
        ts=start_time,
    )


def _run_with_retry(
    *,
    batch_uow: lu.UnitOfWork,
    job: job_spec.JobSpec,
    logger: job_logging_service.AbstractJobLoggingService,
    max_retries: int,
    retries_so_far: int,
    start_time: value_objects.Timestamp,
    ts_adapter: timestamp_adapter.TimestampAdapter,
) -> typing.Tuple[value_objects.Result, value_objects.ExecutionMillis]:
    # noinspection PyBroadException
    try:
        result = job.run(batch_uow, logger) or value_objects.Result.success()
        end_time = ts_adapter.now()
        execution_millis = value_objects.ExecutionMillis.calculate(
            start_time=start_time, end_time=end_time
        )
        return result, execution_millis
    except:
        logger.log_error(traceback.format_exc(10))
        if max_retries > retries_so_far:
            logger.log_info(f"Running retry {retries_so_far} of {max_retries}...")
            return _run_with_retry(
                job=job,
                logger=logger,
                max_retries=max_retries,
                retries_so_far=retries_so_far + 1,
                start_time=start_time,
                ts_adapter=ts_adapter,
                batch_uow=batch_uow,
            )
        else:
            logger.log_info(
                f"[{job.job_name.value}] failed after {max_retries} retries."
            )
            raise
