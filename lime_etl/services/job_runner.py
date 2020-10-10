import datetime
import traceback

import typing

from lime_etl.adapters import timestamp_adapter
from lime_etl.domain import (
    exceptions,
    job_result,
    job_spec,
    job_test_result,
    shared_resource,
    value_objects,
)
from lime_etl.services import job_logging_service, unit_of_work


class JobRunner(typing.Protocol):
    def __call__(
        self,
        *,
        batch_id: value_objects.UniqueId,
        job: job_spec.JobSpec,
        job_id: value_objects.UniqueId,
        logger: job_logging_service.JobLoggingService,
        resources: typing.Mapping[
            value_objects.ResourceName, shared_resource.SharedResource[typing.Any]
        ],
        ts_adapter: timestamp_adapter.TimestampAdapter,
        uow: unit_of_work.UnitOfWork,
        skip_tests: bool,
    ) -> job_result.JobResult:
        ...


def default_job_runner(
    *,
    batch_id: value_objects.UniqueId,
    job: job_spec.JobSpec,
    job_id: value_objects.UniqueId,
    logger: job_logging_service.JobLoggingService,
    resources: typing.Mapping[
        value_objects.ResourceName, shared_resource.SharedResource[typing.Any]
    ],
    ts_adapter: timestamp_adapter.TimestampAdapter,
    uow: unit_of_work.UnitOfWork,
    skip_tests: bool,
) -> job_result.JobResult:
    result = _run_job_pre_handlers(
        batch_id=batch_id,
        job=job,
        job_id=job_id,
        logger=logger,
        resources=resources,
        ts_adapter=ts_adapter,
        uow=uow,
        skip_tests=skip_tests,
    )
    assert result.execution_success_or_failure is not None
    if result.execution_success_or_failure.is_failure:
        new_job = job.on_execution_error(
            result.execution_success_or_failure.failure_message
        )
        if new_job:
            return default_job_runner(
                batch_id=batch_id,
                job=new_job,
                job_id=job_id,
                logger=logger,
                resources=resources,
                ts_adapter=ts_adapter,
                uow=uow,
                skip_tests=skip_tests,
            )
        else:
            return result
    elif any(test.test_failed for test in result.test_results):
        new_job = job.on_test_failure(result.test_results)
        if new_job:
            return default_job_runner(
                batch_id=batch_id,
                job=new_job,
                job_id=job_id,
                logger=logger,
                resources=resources,
                ts_adapter=ts_adapter,
                uow=uow,
                skip_tests=skip_tests,
            )
        else:
            return result
    else:
        return result


def _run_job_pre_handlers(
    *,
    batch_id: value_objects.UniqueId,
    job: job_spec.JobSpec,
    job_id: value_objects.UniqueId,
    logger: job_logging_service.JobLoggingService,
    resources: typing.Mapping[
        value_objects.ResourceName, shared_resource.SharedResource[typing.Any]
    ],
    ts_adapter: timestamp_adapter.TimestampAdapter,
    uow: unit_of_work.UnitOfWork,
    skip_tests: bool,
) -> job_result.JobResult:
    logger.log_info(f"Starting [{job.job_name.value}]...")
    with uow:
        current_batch = uow.batches.get_batch_by_id(batch_id)

    if current_batch is None:
        raise exceptions.BatchNotFound(batch_id)
    else:
        dep_exceptions = {
            jr.job_name
            for jr in current_batch.job_results
            if jr.job_name in job.dependencies
            and jr.execution_success_or_failure is not None
            and jr.execution_success_or_failure.is_failure
        }
        dep_test_failures = {
            jr.job_name
            for jr in current_batch.job_results
            if jr.job_name in job.dependencies and jr.tests_failed
        }
        if dep_exceptions and dep_test_failures:
            errs = ", ".join(sorted(dep_exceptions))  # type: ignore
            test_failures = ", ".join(sorted(dep_test_failures))  # type: ignore
            raise Exception(
                f"The following dependencies failed to execute: {errs} "
                f"and the following jobs had test failures: {test_failures}"
            )
        elif dep_exceptions:
            errs = ", ".join(sorted(dep_exceptions))  # type: ignore
            raise Exception(f"The following dependencies failed to execute: {errs}")
        else:
            result = _run_jobs_with_tests(
                batch_id=batch_id,
                job=job,
                job_id=job_id,
                logger=logger,
                resources=resources,
                ts_adapter=ts_adapter,
                uow=uow,
                skip_tests=skip_tests,
            )
            logger.log_info(f"Finished running [{job.job_name.value}].")
            return result


def _run_jobs_with_tests(
    *,
    batch_id: value_objects.UniqueId,
    job: job_spec.JobSpec,
    job_id: value_objects.UniqueId,
    logger: job_logging_service.JobLoggingService,
    resources: typing.Mapping[
        value_objects.ResourceName, shared_resource.SharedResource[typing.Any]
    ],
    ts_adapter: timestamp_adapter.TimestampAdapter,
    uow: unit_of_work.UnitOfWork,
    skip_tests: bool,
) -> job_result.JobResult:
    result, execution_millis = _run_with_retry(
        job=job,
        logger=logger,
        max_retries=job.max_retries.value,
        resources=resources,
        retries_so_far=0,
        uow=uow,
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
            if isinstance(job, job_spec.AdminJobSpec):
                test_results = job.test(logger=logger, uow=uow)
            elif isinstance(job, job_spec.ETLJobSpec):
                test_results = job.test(logger=logger, resources=resources)
            else:
                raise ValueError(
                    f"Expected either an AdminJobSpec or an ETLJobSpec, but got {job!r}"
                )
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
                        ts=ts_adapter.now(),
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

    ts = ts_adapter.now()
    return job_result.JobResult(
        id=job_id,
        batch_id=batch_id,
        job_name=job.job_name,
        test_results=full_test_results,
        execution_millis=value_objects.ExecutionMillis(execution_millis),
        execution_success_or_failure=value_objects.Result.success(),
        running=value_objects.Flag(False),
        ts=ts,
    )


def _run_with_retry(
    job: job_spec.JobSpec,
    logger: job_logging_service.JobLoggingService,
    max_retries: int,
    resources: typing.Mapping[
        value_objects.ResourceName, shared_resource.SharedResource[typing.Any]
    ],
    retries_so_far: int,
    uow: unit_of_work.UnitOfWork,
) -> typing.Tuple[value_objects.Result, int]:
    # noinspection PyBroadException
    try:
        start_time = datetime.datetime.now()
        result = _run(
            job=job,
            logger=logger,
            resources=resources,
            uow=uow,
        )
        end_time = datetime.datetime.now()
        return result, int((end_time - start_time).total_seconds() * 1000)
    except:
        logger.log_error(traceback.format_exc(10))
        if max_retries > retries_so_far:
            logger.log_info(f"Running retry {retries_so_far} of {max_retries}...")
            return _run_with_retry(
                job=job,
                logger=logger,
                max_retries=max_retries,
                resources=resources,
                retries_so_far=retries_so_far + 1,
                uow=uow,
            )
        else:
            logger.log_info(
                f"[{job.job_name.value}] failed after {max_retries} retries."
            )
            raise


def _run(
    job: job_spec.JobSpec,
    logger: job_logging_service.JobLoggingService,
    uow: unit_of_work.UnitOfWork,
    resources: typing.Mapping[
        value_objects.ResourceName, shared_resource.SharedResource[typing.Any]
    ],
) -> value_objects.Result:
    if isinstance(job, job_spec.AdminJobSpec):
        return job.run(uow=uow, logger=logger) or value_objects.Result.success()
    elif isinstance(job, job_spec.ETLJobSpec):
        return (
            job.run(resources=resources, logger=logger)
            or value_objects.Result.success()
        )
    else:
        raise exceptions.InvalidJobSpec(
            f"Expected an instance of AdminJobSpec or ETLJobSpec, but got {job!r}."
        )
