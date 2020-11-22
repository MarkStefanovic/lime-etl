import datetime
import itertools
import multiprocessing
import traceback
import typing

import lime_uow as lu

import sqlalchemy as sa
from sqlalchemy import orm

from lime_etl import domain, adapters
from lime_etl.services import (
    batch_spec,
    job_spec,
    job_logging_service,
    admin_unit_of_work,
    batch_logging_service,
)

__all__ = (
    "run_batches_in_parallel",
    "run_batch",
)

UoW = typing.TypeVar("UoW", bound=lu.UnitOfWork, contravariant=True)


def run_batches_in_parallel(
    batches: typing.Iterable[batch_spec.BatchSpec[lu.UnitOfWork]],
    admin_engine_uri: str,
    admin_schema: typing.Optional[str] = "etl",
    max_processes: int = 3,
    timeout: typing.Optional[int] = None,
) -> typing.List[domain.BatchStatus]:
    params = [(batch, admin_engine_uri, admin_schema) for batch in batches]
    with multiprocessing.Pool(max_processes) as pool:
        future = pool.starmap_async(run_batch, params)
        return future.get(timeout)


def run_batch(
    batch: batch_spec.BatchSpec[lu.UnitOfWork],
    admin_engine_uri: str,
    admin_schema: typing.Optional[str] = "etl",
) -> domain.BatchStatus:
    start_time = batch.ts_adapter.now()

    admin_engine = sa.create_engine(admin_engine_uri)
    adapters.admin_metadata.create_all(bind=admin_engine)
    adapters.admin_orm.set_schema(schema=domain.SchemaName(admin_schema))
    adapters.admin_orm.start_mappers()
    admin_session_factory = orm.sessionmaker(bind=admin_engine)

    logger = batch_logging_service.BatchLoggingService(
        batch_id=batch.batch_id,
        session=admin_session_factory(),
        ts_adapter=batch.ts_adapter,
    )
    admin_uow = admin_unit_of_work.SqlAlchemyAdminUnitOfWork(
        session_factory=admin_session_factory, ts_adapter=batch.ts_adapter
    )

    try:
        with admin_uow as uow:
            new_batch = domain.BatchStatus(
                id=batch.batch_id,
                name=batch.batch_name,
                job_results=frozenset(),
                execution_millis=None,
                execution_success_or_failure=None,
                running=domain.Flag(True),
                ts=start_time,
            )
            uow.batch_repo.add(new_batch.to_dto())
            uow.save()

        dep_results = check_dependencies(batch.jobs)
        if dep_results:
            raise domain.exceptions.DependencyErrors(dep_results)

        logger.log_info(f"Staring batch [{batch.batch_name.value}]...")
        result = run_batch_or_fail(
            admin_uow=admin_uow,
            batch=batch,
            logger=logger,
            start_time=start_time,
        )

        with admin_uow as uow:
            uow.batch_repo.update(result.to_dto())
            uow.save()

        logger.log_info(f"Batch [{batch.batch_name}] finished.")
        return result
    except Exception as e:
        logger.log_error(str(e))
        end_time = batch.ts_adapter.now()
        with admin_uow as uow:
            result = domain.BatchStatus(
                id=batch.batch_id,
                name=batch.batch_name,
                job_results=frozenset(),
                execution_success_or_failure=domain.Result.failure(str(e)),
                execution_millis=domain.ExecutionMillis.calculate(
                    start_time=start_time, end_time=end_time
                ),
                running=domain.Flag(False),
                ts=start_time,
            )
            uow.batch_repo.update(result.to_dto())
            uow.save()
        raise
    finally:
        admin_uow.close()
        batch.uow.close()


def run_batch_or_fail(
    *,
    admin_uow: admin_unit_of_work.AdminUnitOfWork,
    batch: batch_spec.BatchSpec[lu.UnitOfWork],
    logger: batch_logging_service.AbstractBatchLoggingService,
    start_time: domain.Timestamp,
) -> domain.BatchStatus:
    check_for_duplicate_job_names(batch.jobs)

    job_results: typing.List[domain.JobResult] = []
    for ix, job in enumerate(batch.jobs):
        job_id = domain.UniqueId.generate()

        if job.dependencies and all(
            isinstance(r.status, (domain.JobSkipped, domain.JobFailed))
            for r in job_results
            if r.job_name in job.dependencies
        ):
            logger.log_info(
                f"All the dependencies for [{job.job_name.value}] were skipped or failed so the job has been skipped."
            )
            result = domain.JobResult(
                id=job_id,
                batch_id=batch.batch_id,
                job_name=job.job_name,
                test_results=frozenset(),
                execution_millis=domain.ExecutionMillis(0),
                status=domain.JobStatus.skipped("Dependencies were skipped or failed."),
                ts=start_time,
            )
        else:
            current_ts = batch.ts_adapter.now()
            with admin_uow as uow:
                last_ts = uow.job_repo.get_last_successful_ts(job.job_name)

            if last_ts:
                seconds_since_last_refresh = (
                    current_ts.value - last_ts.value
                ).total_seconds()
                time_to_run_again = (
                    seconds_since_last_refresh < job.min_seconds_between_refreshes.value
                )
            else:
                seconds_since_last_refresh = 0
                time_to_run_again = True

            if time_to_run_again:
                job_logger = logger.create_job_logger()
                result = domain.JobResult.running(
                    job_status_id=job_id,
                    batch_id=batch.batch_id,
                    job_name=job.job_name,
                    ts=start_time,
                )
                with admin_uow as uow:
                    uow.job_repo.add(result.to_dto())
                    uow.save()

                # noinspection PyBroadException
                try:
                    result = run_job(
                        admin_uow=admin_uow,
                        batch=batch,
                        job=job,
                        logger=job_logger,
                        job_id=job_id,
                    )
                except Exception as e:
                    millis = batch.ts_adapter.get_elapsed_time(start_time)
                    logger.log_error(str(traceback.format_exc(10)))
                    result = domain.JobResult(
                        id=job_id,
                        batch_id=batch.batch_id,
                        job_name=job.job_name,
                        test_results=frozenset(),
                        execution_millis=millis,
                        status=domain.JobStatus.failed(str(e)),
                        ts=result.ts,
                    )
            else:
                logger.log_info(
                    f"[{job.job_name.value}] was run successfully {seconds_since_last_refresh:.0f} seconds "
                    f"ago and it is set to refresh every {job.min_seconds_between_refreshes.value} seconds, "
                    f"so there is no need to refresh again."
                )
                result = domain.JobResult(
                    id=job_id,
                    batch_id=batch.batch_id,
                    job_name=job.job_name,
                    test_results=frozenset(),
                    execution_millis=domain.ExecutionMillis(0),
                    status=domain.JobStatus.skipped(
                        f"The job ran {seconds_since_last_refresh:.0f} seconds ago, so it is not time yet."
                    ),
                    ts=start_time,
                )

        job_results.append(result)
        with admin_uow as uow:
            uow.job_repo.update(result.to_dto())
            uow.save()

    end_time = batch.ts_adapter.now()

    execution_millis = int((end_time.value - start_time.value).total_seconds() * 1000)
    return domain.BatchStatus(
        id=batch.batch_id,
        name=batch.batch_name,
        execution_millis=domain.ExecutionMillis(execution_millis),
        job_results=frozenset(job_results),
        execution_success_or_failure=domain.Result.success(),
        running=domain.Flag(False),
        ts=end_time,
    )


def run_job(
    *,
    admin_uow: admin_unit_of_work.AdminUnitOfWork,
    batch: batch_spec.BatchSpec[UoW],
    job: job_spec.JobSpec[UoW],
    job_id: domain.UniqueId,
    logger: job_logging_service.AbstractJobLoggingService,
) -> domain.JobResult:
    result: domain.JobResult = run_job_pre_handlers(
        admin_uow=admin_uow,
        batch=batch,
        job=job,
        job_id=job_id,
        logger=logger,
    )
    assert result.status is not None
    if isinstance(result.status, domain.JobFailed):
        new_job = job.on_execution_error(result.status.error_message.value)
        if new_job:
            return run_job(
                admin_uow=admin_uow,
                batch=batch,
                job=new_job,
                job_id=job_id,
                logger=logger,
            )
        else:
            return result
    elif any(test.test_failed for test in result.test_results):
        new_job = job.on_test_failure(result.test_results)
        if new_job:
            return run_job(
                admin_uow=admin_uow,
                batch=batch,
                job=new_job,
                job_id=job_id,
                logger=logger,
            )
        else:
            return result
    else:
        return result


def run_job_pre_handlers(
    *,
    admin_uow: admin_unit_of_work.AdminUnitOfWork,
    batch: batch_spec.BatchSpec[UoW],
    job: job_spec.JobSpec[UoW],
    job_id: domain.UniqueId,
    logger: job_logging_service.AbstractJobLoggingService,
) -> domain.JobResult:
    logger.log_info(f"Starting [{job.job_name.value}]...")
    start_time = batch.ts_adapter.now()
    with admin_uow as uow:
        current_batch = uow.batch_repo.get(batch.batch_id.value).to_domain()

    if current_batch is None:
        raise domain.exceptions.BatchNotFound(batch.batch_id)

    dep_exceptions = {
        jr.job_name.value
        for jr in current_batch.job_results
        if jr.job_name in job.dependencies
        and jr.status is not None
        and isinstance(jr.status, domain.JobFailed)
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
        result = run_jobs_with_tests(
            batch=batch,
            job=job,
            job_id=job_id,
            logger=logger,
            start_time=start_time,
        )
        logger.log_info(f"Finished running [{job.job_name.value}].")
        return result


def run_jobs_with_tests(
    *,
    batch: batch_spec.BatchSpec[UoW],
    job: job_spec.JobSpec[UoW],
    job_id: domain.UniqueId,
    logger: job_logging_service.AbstractJobLoggingService,
    start_time: domain.Timestamp,
) -> domain.JobResult:
    result, execution_millis = run_job_with_retry(
        batch=batch,
        job=job,
        logger=logger,
        max_retries=job.max_retries.value,
        retries_so_far=0,
        start_time=start_time,
    )
    if isinstance(result, domain.JobRanSuccessfully):
        logger.log_info(f"[{job.job_name.value}] finished successfully.")
        logger.log_info(f"Running the tests for [{job.job_name.value}]...")
        test_start_time = datetime.datetime.now()
        test_results = job.test(batch.uow, logger)  # type: ignore
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
                domain.JobTestResult(
                    id=domain.UniqueId.generate(),
                    job_id=job_id,
                    test_name=test_result.test_name,
                    test_success_or_failure=test_result.test_success_or_failure,
                    execution_millis=domain.ExecutionMillis(test_execution_millis),
                    execution_success_or_failure=domain.Result.success(),
                    ts=start_time,
                )
                for test_result in test_results
            )
        else:
            logger.log_info("The job test method returned no results.")
            full_test_results = frozenset()
    elif isinstance(result, domain.JobFailed):
        logger.log_info(
            f"An exception occurred while running [{job.job_name.value}]: "
            f"{result.error_message}."
        )
        full_test_results = frozenset()
    elif isinstance(result, domain.JobSkipped):
        logger.log_info(f"[{job.job_name.value}] was skipped.")
        full_test_results = frozenset()
    else:
        raise TypeError(f"Unrecognized job result: {result!r}")

    return domain.JobResult(
        id=job_id,
        batch_id=batch.batch_id,
        job_name=job.job_name,
        test_results=full_test_results,
        execution_millis=execution_millis,
        status=domain.JobStatus.success(),
        ts=start_time,
    )


def run_job_with_retry(
    *,
    batch: batch_spec.BatchSpec[UoW],
    job: job_spec.JobSpec[UoW],
    logger: job_logging_service.AbstractJobLoggingService,
    max_retries: int,
    retries_so_far: int,
    start_time: domain.Timestamp,
) -> typing.Tuple[domain.JobStatus, domain.ExecutionMillis]:
    # noinspection PyBroadException
    try:
        result = job.run(batch.uow, logger) or domain.JobStatus.success()  # type: ignore
        end_time = batch.ts_adapter.now()
        execution_millis = domain.ExecutionMillis.calculate(
            start_time=start_time, end_time=end_time
        )
        return result, execution_millis
    except:
        logger.log_error(traceback.format_exc(10))
        if max_retries > retries_so_far:
            logger.log_info(f"Running retry {retries_so_far} of {max_retries}...")
            return run_job_with_retry(
                batch=batch,
                job=job,
                logger=logger,
                max_retries=max_retries,
                retries_so_far=retries_so_far + 1,
                start_time=start_time,
            )
        else:
            logger.log_info(
                f"[{job.job_name.value}] failed after {max_retries} retries."
            )
            raise


def check_for_duplicate_job_names(
    jobs: typing.Collection[job_spec.JobSpec[lu.UnitOfWork]], /
) -> None:
    job_names = [job.job_name for job in jobs]
    duplicates = {
        job_name: ct for job_name in job_names if (ct := job_names.count(job_name)) > 1
    }
    if duplicates:
        raise domain.exceptions.DuplicateJobNamesError(duplicates)


def check_dependencies(
    jobs: typing.List[job_spec.JobSpec[lu.UnitOfWork]], /
) -> typing.Set[domain.JobDependencyErrors]:
    job_names = {job.job_name for job in jobs}
    unresolved_dependencies_by_table = {
        job.job_name: set(dep for dep in job.dependencies if dep not in job_names)
        for job in jobs
        if any(dep not in job_names for dep in job.dependencies)
    }
    unresolved_dependencies = {
        dep for dep_grp in unresolved_dependencies_by_table.values() for dep in dep_grp
    }

    job_names_seen_so_far: typing.List[domain.JobName] = []
    jobs_out_of_order_by_table: typing.Dict[
        domain.JobName, typing.Set[domain.JobName]
    ] = dict()
    for job in jobs:
        job_names_seen_so_far.append(job.job_name)
        job_deps_out_of_order = []
        for dep in job.dependencies:
            if dep not in job_names_seen_so_far and dep not in unresolved_dependencies:
                job_deps_out_of_order.append(dep)
        if job_deps_out_of_order:
            jobs_out_of_order_by_table[job.job_name] = set(job_deps_out_of_order)

    return {
        domain.JobDependencyErrors(
            job_name=job_name,
            missing_dependencies=frozenset(
                unresolved_dependencies_by_table.get(job_name, set())
            ),
            jobs_out_of_order=frozenset(
                jobs_out_of_order_by_table.get(job_name, set())
            ),
        )
        for job_name in set(
            itertools.chain(
                unresolved_dependencies_by_table.keys(),
                jobs_out_of_order_by_table.keys(),
            )
        )
    }
