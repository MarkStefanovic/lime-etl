import datetime
import itertools
import multiprocessing
import traceback
import typing

import lime_uow as lu
import sqlalchemy as sa
from sqlalchemy import orm

from lime_etl import domain, adapter
from lime_etl.service import admin

__all__ = (
    "run_admin",
    "run_batch",
    "run_batches_in_parallel",
)

UoW = typing.TypeVar("UoW", bound=lu.UnitOfWork, contravariant=True)


def run_admin(
    *,
    admin_engine_uri: domain.DbUri,
    admin_schema: domain.SchemaName = domain.SchemaName("etl"),
    days_logs_to_keep: domain.DaysToKeep = domain.DaysToKeep(3),
    ts_adapter: domain.TimestampAdapter = adapter.LocalTimestampAdapter(),
) -> domain.BatchStatus:
    batch = admin.AdminBatch(
        admin_engine_uri=admin_engine_uri,
        admin_schema=admin_schema,
        days_logs_to_keep=days_logs_to_keep,
        ts_adapter=ts_adapter,
    )
    return batch.run()


def run_batches_in_parallel(
    batches: typing.Iterable[domain.BatchSpec[UoW]],
    admin_engine_uri: domain.DbUri,
    admin_schema: domain.SchemaName = domain.SchemaName("etl"),
    max_processes: domain.MaxProcesses = domain.MaxProcesses(None),
    timeout: domain.TimeoutSeconds = domain.TimeoutSeconds(None),
    ts_adapter: domain.TimestampAdapter = adapter.LocalTimestampAdapter(),
) -> typing.List[domain.BatchStatus]:
    params = [(batch, admin_engine_uri, admin_schema, ts_adapter) for batch in batches]
    with multiprocessing.Pool(max_processes.value, maxtasksperchild=1) as pool:
        future = pool.starmap_async(run_batch, params)
        return future.get(timeout.value)


def run_batch(
    batch: domain.BatchSpec[UoW],
    admin_engine_uri: domain.DbUri,
    admin_schema: domain.SchemaName = domain.SchemaName("etl"),
    ts_adapter: domain.TimestampAdapter = adapter.LocalTimestampAdapter(),
) -> domain.BatchStatus:
    start_time = ts_adapter.now()

    admin_engine = sa.create_engine(admin_engine_uri.value)
    adapter.admin_metadata.create_all(bind=admin_engine)
    adapter.admin_orm.set_schema(schema=admin_schema)
    adapter.admin_orm.start_mappers()
    admin_session_factory = orm.sessionmaker(bind=admin_engine)

    logger = adapter.SqlAlchemyBatchLogger(
        batch_id=batch.batch_id,
        session=admin_session_factory(),
        ts_adapter=ts_adapter,
    )
    admin_uow: domain.AdminUnitOfWork = adapter.SqlAlchemyAdminUnitOfWork(
        session_factory=admin_session_factory, ts_adapter=ts_adapter
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

        logger.log_info(f"Staring batch [{batch.batch_name.value}]...")
        batch_uow = batch.create_uow()
        try:
            result = run_batch_or_fail(
                admin_uow=admin_uow,
                batch=batch,
                batch_uow=batch_uow,
                logger=logger,
                start_time=start_time,
                ts_adapter=ts_adapter,
            )
        finally:
            batch_uow.close()

        with admin_uow as uow:
            uow.batch_repo.update(result.to_dto())
            uow.save()

        logger.log_info(f"Batch [{batch.batch_name}] finished.")
        return result
    except Exception as e:
        logger.log_error(traceback.format_exc(10))
        end_time = ts_adapter.now()
        with admin_uow as uow:
            result = domain.BatchStatus(
                id=batch.batch_id,
                name=batch.batch_name,
                job_results=frozenset(),
                execution_success_or_failure=domain.Result.failure(f"{e}\n{traceback.format_exc(10)}"),
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


def run_batch_or_fail(
    *,
    admin_uow: domain.admin_unit_of_work.AdminUnitOfWork,
    batch: domain.BatchSpec[UoW],
    batch_uow: UoW,
    logger: domain.batch_logger.BatchLogger,
    start_time: domain.Timestamp,
    ts_adapter: domain.TimestampAdapter,
) -> domain.BatchStatus:
    jobs = batch.create_jobs(batch_uow)
    check_dependencies(jobs)
    check_for_duplicate_job_names(jobs)

    job_results: typing.List[domain.JobResult] = []
    for ix, job in enumerate(jobs):
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
            current_ts = ts_adapter.now()
            with admin_uow as uow:
                last_ts = uow.job_repo.get_last_successful_ts(job.job_name)

            if last_ts:
                seconds_since_last_refresh = (
                    current_ts.value - last_ts.value
                ).total_seconds()
                time_to_run_again = (
                    seconds_since_last_refresh > job.min_seconds_between_refreshes.value
                )
            else:
                seconds_since_last_refresh = 0
                time_to_run_again = True

            if time_to_run_again:
                job_logger = logger.create_job_logger(job_id)
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
                        batch_uow=batch_uow,
                        job=job,
                        logger=job_logger,
                        job_id=job_id,
                        ts_adapter=ts_adapter,
                    )
                except Exception as e:
                    millis = ts_adapter.get_elapsed_time(start_time)
                    err_msg = f"{e}\n{traceback.format_exc(10)}"
                    result = domain.JobResult(
                        id=job_id,
                        batch_id=batch.batch_id,
                        job_name=job.job_name,
                        test_results=frozenset(),
                        execution_millis=millis,
                        status=domain.JobStatus.failed(err_msg),
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

    end_time = ts_adapter.now()

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
    admin_uow: domain.admin_unit_of_work.AdminUnitOfWork,
    batch: domain.BatchSpec[UoW],
    batch_uow: UoW,
    job: domain.JobSpec[UoW],
    job_id: domain.UniqueId,
    logger: domain.JobLogger,
    ts_adapter: domain.TimestampAdapter,
) -> domain.JobResult:
    result: domain.JobResult = run_job_pre_handlers(
        admin_uow=admin_uow,
        batch=batch,
        batch_uow=batch_uow,
        job=job,
        job_id=job_id,
        logger=logger,
        ts_adapter=ts_adapter,
    )
    assert result.status is not None
    if isinstance(result.status, domain.JobFailed):
        new_job = job.on_execution_error(result.status.error_message.value)
        if new_job:
            return run_job(
                admin_uow=admin_uow,
                batch=batch,
                batch_uow=batch_uow,
                job=new_job,
                job_id=job_id,
                logger=logger,
                ts_adapter=ts_adapter,
            )
        else:
            return result
    elif any(test.test_failed for test in result.test_results):
        new_job = job.on_test_failure(result.test_results)
        if new_job:
            return run_job(
                admin_uow=admin_uow,
                batch=batch,
                batch_uow=batch_uow,
                job=new_job,
                job_id=job_id,
                logger=logger,
                ts_adapter=ts_adapter,
            )
        else:
            return result
    else:
        return result


def run_job_pre_handlers(
    *,
    admin_uow: domain.admin_unit_of_work.AdminUnitOfWork,
    batch: domain.BatchSpec[UoW],
    batch_uow: UoW,
    job: domain.JobSpec[UoW],
    job_id: domain.UniqueId,
    logger: domain.JobLogger,
    ts_adapter: domain.TimestampAdapter,
) -> domain.JobResult:
    logger.log_info(f"Starting [{job.job_name.value}]...")
    start_time = ts_adapter.now()
    with admin_uow as uow:
        current_batch = uow.batch_repo.get(batch.batch_id.value).to_domain()

    if current_batch is None:
        msg = f"The batch [{batch.batch_id.value}] was not found"
        raise domain.exceptions.BatchNotFound(msg)

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
            admin_uow=admin_uow,
            batch=batch,
            batch_uow=batch_uow,
            job=job,
            job_id=job_id,
            logger=logger,
            start_time=start_time,
            ts_adapter=ts_adapter,
        )
        logger.log_info(f"Finished running [{job.job_name.value}].")
        return result


def run_jobs_with_tests(
    *,
    admin_uow: domain.AdminUnitOfWork,
    batch: domain.BatchSpec[UoW],
    batch_uow: UoW,
    job: domain.JobSpec[UoW],
    job_id: domain.UniqueId,
    logger: domain.JobLogger,
    start_time: domain.Timestamp,
    ts_adapter: domain.TimestampAdapter,
) -> domain.JobResult:
    result, execution_millis = run_job_with_retry(
        admin_uow=admin_uow,
        batch=batch,
        batch_uow=batch_uow,
        job=job,
        logger=logger,
        max_retries=job.max_retries.value,
        retries_so_far=0,
        start_time=start_time,
        ts_adapter=ts_adapter,
    )
    if isinstance(result, domain.JobRanSuccessfully):
        logger.log_info(f"[{job.job_name.value}] finished successfully.")

        if batch.skip_tests.value:
            full_test_results: typing.FrozenSet[domain.JobTestResult] = frozenset()
        else:
            with admin_uow:
                last_test_results = admin_uow.job_repo.latest_test_results(job.job_name)

            if last_test_results:
                last_test_time: datetime.datetime = max(
                    t.ts.value for t in last_test_results
                )
                now: datetime.datetime = ts_adapter.now().value
                seconds_since_last_test_run: int = int(
                    (now - last_test_time).total_seconds()
                )
                if seconds_since_last_test_run >= job.min_seconds_between_tests.value:
                    skip_tests = False
                    logger.log_info(
                        f"The tests for [{job.job_name.value}] were last run {seconds_since_last_test_run} seconds "
                        f"ago, and they are set to run every {job.min_seconds_between_tests.value}, so they will "
                        f"be run again now."
                    )
                else:
                    skip_tests = True
                    logger.log_info(
                        f"The tests for [{job.job_name.value}] were run {seconds_since_last_test_run} seconds ago, "
                        f"and they are set to run every {job.min_seconds_between_tests.value} so they are not "
                        f"ready to be run again."
                    )
            else:
                skip_tests = False
                logger.log_info(
                    f"The tests for [{job.job_name.value}] have not been run before, so they will be run now."
                )

            if skip_tests:
                full_test_results = frozenset()
            else:
                test_start_time = datetime.datetime.now()
                test_results = job.test(admin_uow, batch_uow, logger)
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
        status=result,
        ts=start_time,
    )


def run_job_with_retry(
    *,
    admin_uow: domain.AdminUnitOfWork,
    batch: domain.BatchSpec[UoW],
    batch_uow: UoW,
    job: domain.JobSpec[UoW],
    logger: domain.JobLogger,
    max_retries: int,
    retries_so_far: int,
    start_time: domain.Timestamp,
    ts_adapter: domain.TimestampAdapter,
) -> typing.Tuple[domain.JobStatus, domain.ExecutionMillis]:
    # noinspection PyBroadException
    try:
        result = job.run(admin_uow, batch_uow, logger) or domain.JobStatus.success()
        end_time = ts_adapter.now()
        execution_millis = domain.ExecutionMillis.calculate(
            start_time=start_time, end_time=end_time
        )
        return result, execution_millis
    except:
        if max_retries > retries_so_far:
            logger.log_info(f"Running retry {retries_so_far} of {max_retries}...")
            return run_job_with_retry(
                admin_uow=admin_uow,
                batch=batch,
                batch_uow=batch_uow,
                job=job,
                logger=logger,
                max_retries=max_retries,
                retries_so_far=retries_so_far + 1,
                start_time=start_time,
                ts_adapter=ts_adapter,
            )
        else:
            logger.log_info(
                f"[{job.job_name.value}] failed after {max_retries} retries."
            )
            raise


def check_for_duplicate_job_names(
    jobs: typing.Collection[domain.JobSpec[UoW]], /
) -> None:
    job_names = [job.job_name for job in jobs]
    duplicates = {
        job_name: ct for job_name in job_names if (ct := job_names.count(job_name)) > 1
    }
    if duplicates:
        raise domain.exceptions.DuplicateJobNamesError(duplicates)


def check_dependencies(jobs: typing.List[domain.JobSpec[UoW]], /) -> None:
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

    dependency_errors = {
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
    if dependency_errors:
        raise domain.exceptions.DependencyErrors(dependency_errors)
