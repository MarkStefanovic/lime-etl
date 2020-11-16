import itertools
import traceback
import typing

import lime_uow as lu

from lime_etl import domain, adapters
from lime_etl.services import (
    admin_unit_of_work,
    batch_logging_service,
    job_spec,
    job_runner,
)

__all__ = ("run",)


def run(
    *,
    admin_uow: admin_unit_of_work.AdminUnitOfWork,
    batch_id: domain.UniqueId,
    batch_name: domain.BatchName,
    batch_uow: lu.UnitOfWork,
    jobs: typing.Collection[job_spec.JobSpec],
    logger: batch_logging_service.BatchLoggingService,
    skip_tests: bool,
    ts_adapter: adapters.TimestampAdapter,
) -> domain.BatchStatus:
    start_time = ts_adapter.now()
    try:
        dep_results = check_dependencies(jobs)
        if dep_results:
            raise domain.exceptions.DependencyErrors(dep_results)

        with admin_uow as uow:
            new_batch = domain.BatchStatus(
                id=batch_id,
                name=batch_name,
                job_results=frozenset(),
                execution_millis=None,
                execution_success_or_failure=None,
                running=domain.Flag(True),
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
            result = domain.BatchStatus(
                id=batch_id,
                name=batch_name,
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
        batch_uow.close()


def check_dependencies(
    jobs: typing.Collection[job_spec.JobSpec], /
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


def _check_for_duplicate_job_names(
    jobs: typing.Collection[job_spec.JobSpec], /
) -> None:
    job_names = [job.job_name for job in jobs]
    duplicates = {
        job_name: ct for job_name in job_names if (ct := job_names.count(job_name)) > 1
    }
    if duplicates:
        raise domain.exceptions.DuplicateJobNamesError(duplicates)


def _run_batch(
    *,
    admin_uow: admin_unit_of_work.AdminUnitOfWork,
    batch_id: domain.UniqueId,
    batch_logger: batch_logging_service.AbstractBatchLoggingService,
    batch_name: domain.BatchName,
    batch_uow: lu.UnitOfWork,
    jobs: typing.Collection[job_spec.JobSpec],
    skip_tests: bool,
    start_time: domain.Timestamp,
    ts_adapter: adapters.TimestampAdapter,
) -> domain.BatchStatus:
    _check_for_duplicate_job_names(jobs)

    job_results: typing.List[domain.JobResult] = []
    for ix, job in enumerate(jobs):
        job_id = domain.UniqueId.generate()

        if job.dependencies and all(
            isinstance(r.status, (domain.JobSkipped, domain.JobFailed))
            for r in job_results
            if r.job_name in job.dependencies
        ):
            batch_logger.log_info(
                f"All the dependencies for [{job.job_name.value}] were skipped or failed so the job has been skipped."
            )
            result = domain.JobResult(
                id=job_id,
                batch_id=batch_id,
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
                seconds_since_last_refresh = (current_ts.value - last_ts.value).total_seconds()
                time_to_run_again =  seconds_since_last_refresh < job.min_seconds_between_refreshes.value
            else:
                seconds_since_last_refresh = 0
                time_to_run_again = True

            if time_to_run_again:
                job_logger = batch_logger.create_job_logger()
                result = domain.JobResult.running(
                    job_status_id=job_id,
                    batch_id=batch_id,
                    job_name=job.job_name,
                    ts=start_time,
                )
                with admin_uow as uow:
                    uow.job_repo.add(result.to_dto())
                    uow.save()

                # noinspection PyBroadException
                try:
                    result = job_runner.run_job(
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
                    batch_logger.log_error(str(traceback.format_exc(10)))
                    result = domain.JobResult(
                        id=job_id,
                        batch_id=batch_id,
                        job_name=job.job_name,
                        test_results=frozenset(),
                        execution_millis=millis,
                        status=domain.JobStatus.failed(str(e)),
                        ts=result.ts,
                    )
            else:
                batch_logger.log_info(
                    f"[{job.job_name.value}] was run successfully {seconds_since_last_refresh:.0f} seconds "
                    f"ago and it is set to refresh every {job.min_seconds_between_refreshes.value} seconds, "
                    f"so there is no need to refresh again."
                )
                result = domain.JobResult(
                    id=job_id,
                    batch_id=batch_id,
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
            admin_uow.save()

    end_time = ts_adapter.now()

    execution_millis = int((end_time.value - start_time.value).total_seconds() * 1000)
    return domain.BatchStatus(
        id=batch_id,
        name=batch_name,
        execution_millis=domain.ExecutionMillis(execution_millis),
        job_results=frozenset(job_results),
        execution_success_or_failure=domain.Result.success(),
        running=domain.Flag(False),
        ts=end_time,
    )
