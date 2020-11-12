import itertools
import traceback
import typing

import lime_uow as lu

from lime_etl.adapters import timestamp_adapter
from lime_etl.domain import (
    batch_result,
    exceptions,
    job_dependency_errors,
    job_result, value_objects,
)
from lime_etl.services import (
    admin_unit_of_work,
    batch_logging_service,
    job_runner,
    job_spec,
)

__all__ = ("run",)


def run(
    *,
    admin_uow: admin_unit_of_work.AdminUnitOfWork,
    batch_id: value_objects.UniqueId,
    batch_name: value_objects.BatchName,
    batch_uow: lu.UnitOfWork,
    jobs: typing.Collection[job_spec.JobSpec],
    logger: batch_logging_service.BatchLoggingService,
    skip_tests: bool,
    ts_adapter: timestamp_adapter.TimestampAdapter,
) -> batch_result.BatchResult:
    start_time = ts_adapter.now()
    try:
        dep_results = check_dependencies(jobs)
        if dep_results:
            raise exceptions.DependencyErrors(dep_results)

        with admin_uow as uow:
            new_batch = batch_result.BatchResult(
                id=batch_id,
                name=batch_name,
                job_results=frozenset(),
                execution_millis=None,
                execution_success_or_failure=None,
                running=value_objects.Flag(True),
                ts=start_time,
            )
            uow.batch_repo.add(new_batch.to_dto())
            uow.save()

        logger.log_info(f"Staring batch [{batch_name.value}]...")
        result = _run_batch(
            admin_uow=admin_uow,
            batch_id=batch_id,
            batch_logger=logger,
            batch_name=batch_name,
            batch_uow=batch_uow,
            jobs=jobs,
            skip_tests=skip_tests,
            start_time=start_time,
            ts_adapter=ts_adapter,
        )

        with admin_uow as uow:
            uow.batch_repo.update(result.to_dto())
            uow.save()

        logger.log_info(f"Batch [{batch_name}] finished.")
        return result
    except Exception as e:
        logger.log_error(str(e))
        end_time = ts_adapter.now()
        with admin_uow as uow:
            result = batch_result.BatchResult(
                id=batch_id,
                name=batch_name,
                job_results=frozenset(),
                execution_success_or_failure=value_objects.Result.failure(str(e)),
                execution_millis=value_objects.ExecutionMillis.calculate(
                    start_time=start_time, end_time=end_time
                ),
                running=value_objects.Flag(False),
                ts=start_time,
            )
            uow.batch_repo.update(result.to_dto())
            uow.save()
        raise
    finally:
        admin_uow.close()
        batch_uow.close()


def check_dependencies(
    jobs: typing.Collection[job_spec.JobSpec], /
) -> typing.Set[job_dependency_errors.JobDependencyErrors]:
    job_names = {job.job_name for job in jobs}
    unresolved_dependencies_by_table = {
        job.job_name: set(dep for dep in job.dependencies if dep not in job_names)
        for job in jobs
        if any(dep not in job_names for dep in job.dependencies)
    }
    unresolved_dependencies = {
        dep for dep_grp in unresolved_dependencies_by_table.values() for dep in dep_grp
    }

    job_names_seen_so_far: typing.List[value_objects.JobName] = []
    jobs_out_of_order_by_table: typing.Dict[
        value_objects.JobName, typing.Set[value_objects.JobName]
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
        job_dependency_errors.JobDependencyErrors(
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


def _check_for_duplicate_job_names(
    jobs: typing.Collection[job_spec.JobSpec], /
) -> None:
    job_names = [job.job_name for job in jobs]
    duplicates = {
        job_name: ct for job_name in job_names if (ct := job_names.count(job_name)) > 1
    }
    if duplicates:
        raise exceptions.DuplicateJobNamesError(duplicates)


def _run_batch(
    *,
    admin_uow: admin_unit_of_work.AdminUnitOfWork,
    batch_id: value_objects.UniqueId,
    batch_logger: batch_logging_service.AbstractBatchLoggingService,
    batch_name: value_objects.BatchName,
    batch_uow: lu.UnitOfWork,
    jobs: typing.Collection[job_spec.JobSpec],
    skip_tests: bool,
    start_time: value_objects.Timestamp,
    ts_adapter: timestamp_adapter.TimestampAdapter,
) -> batch_result.BatchResult:
    _check_for_duplicate_job_names(jobs)

    job_results: typing.List[job_result.JobResult] = []
    for ix, job in enumerate(jobs):
        current_ts = ts_adapter.now()
        with admin_uow as uow:
            last_ts = uow.job_repo.get_last_successful_ts(job.job_name)

        if last_ts:
            seconds_since_last_refresh = (
                current_ts.value - last_ts.value
            ).total_seconds()
            if seconds_since_last_refresh < job.min_seconds_between_refreshes.value:
                batch_logger.log_info(
                    f"[{job.job_name.value}] was run successfully {seconds_since_last_refresh:.0f} seconds "
                    f"ago and it is set to refresh every {job.min_seconds_between_refreshes.value} seconds, "
                    f"so there is no need to refresh again."
                )
                continue

        job_id = value_objects.UniqueId.generate()
        job_logger = batch_logger.create_job_logger()
        result = job_result.JobResult(
            id=job_id,
            batch_id=batch_id,
            job_name=job.job_name,
            test_results=frozenset(),
            execution_millis=None,
            execution_success_or_failure=None,
            running=value_objects.Flag(True),
            ts=start_time,
        )
        with admin_uow as uow:
            uow.job_repo.add(result.to_dto())
            uow.save()

        # noinspection PyBroadException
        try:
            result = job_runner.default_job_runner(
                admin_uow=admin_uow,
                batch_uow=batch_uow,
                job=job,
                logger=job_logger,
                batch_id=batch_id,
                job_id=job_id,
                skip_tests=skip_tests,
                ts_adapter=ts_adapter,
            )
        except Exception as e:
            millis = ts_adapter.get_elapsed_time(start_time)
            err = value_objects.Result.failure(str(e))
            batch_logger.log_error(str(traceback.format_exc(10)))
            result = job_result.JobResult(
                id=job_id,
                batch_id=batch_id,
                job_name=job.job_name,
                test_results=frozenset(),
                execution_millis=millis,
                execution_success_or_failure=err,
                running=value_objects.Flag(False),
                ts=result.ts,
            )
        finally:
            assert result is not None
            job_results.append(result)
            with admin_uow as uow:
                uow.job_repo.update(result.to_dto())
                admin_uow.save()

    end_time = ts_adapter.now()

    execution_millis = int((end_time.value - start_time.value).total_seconds() * 1000)
    return batch_result.BatchResult(
        id=batch_id,
        name=batch_name,
        execution_millis=value_objects.ExecutionMillis(execution_millis),
        job_results=frozenset(job_results),
        execution_success_or_failure=value_objects.Result.success(),
        running=value_objects.Flag(False),
        ts=end_time,
    )
