from typing import List, Protocol, runtime_checkable

import typing

from lime_etl.adapters import timestamp_adapter
from lime_etl.domain import batch_delta
from lime_etl.domain import batch, job_result, job_spec, value_objects
from lime_etl.services import (
    batch_logging_service,
    job_runner,
)
from lime_etl.services import job_logging_service, unit_of_work


@runtime_checkable
class BatchRunner(Protocol):
    def __call__(
        self,
        uow: unit_of_work.UnitOfWork,
        batch_id: value_objects.UniqueId,
        jobs: typing.Collection[job_spec.JobSpec],
        batch_logger: batch_logging_service.BatchLoggingService,
        ts_adapter: timestamp_adapter.TimestampAdapter,
    ) -> batch.Batch:
        ...


def run(
    uow: unit_of_work.UnitOfWork,
    jobs: typing.Collection[job_spec.JobSpec],
    ts_adapter: timestamp_adapter.TimestampAdapter,
) -> batch_delta.BatchDelta:
    batch_id = value_objects.UniqueId.generate()
    batch_logger = batch_logging_service.DefaultBatchLoggingService(
        uow=uow, batch_id=batch_id
    )
    dep_results = check_dependencies(jobs)
    if dep_results.is_failure:
        batch_logger.log_error(value_objects.LogMessage(dep_results.failure_message))
        raise Exception(dep_results.failure_message)

    try:
        with uow:
            previous_results = uow.batches.get_latest()
            new_batch = batch.Batch(
                id=batch_id,
                job_results=frozenset(),
                execution_millis=None,
                execution_success_or_failure=None,
                running=value_objects.Flag(True),
                ts=ts_adapter.now(),
            )
            uow.batches.add(new_batch)
            uow.commit()

        batch_logger.log_info(
            value_objects.LogMessage(f"Staring batch [{batch_id.value}]...")
        )
        result = _run_batch(
            batch_logger=batch_logger,
            uow=uow,
            batch_id=batch_id,
            jobs=jobs,
            ts_adapter=ts_adapter,
        )
        uow.batches.update(result)
        batch_logger.log_info(
            value_objects.LogMessage(f"Batch [{batch_id.value}] finished.")
        )
        return batch_delta.BatchDelta(
            current_results=result,
            previous_results=previous_results,
        )
    except Exception as e:
        batch_logger.log_error(value_objects.LogMessage(str(e)))
        raise


def check_dependencies(
    jobs: typing.Collection[job_spec.JobSpec], /
) -> value_objects.Result:
    dep_issues: typing.List[str] = []

    job_names: typing.Set[value_objects.JobName] = {job.job_name for job in jobs}
    missing_deps_by_table: typing.Dict[str, typing.List[value_objects.JobName]] = {
        job.job_name.value: sorted(
            dep for dep in job.dependencies if dep not in job_names
        )
        for job in jobs
        if any(dep not in job_names for dep in job.dependencies)
    }
    dep_issues += [
        f"{job_name} has the following unresolved dependencies: {', '.join(dep.value for dep in deps)}."
        for job_name, deps in missing_deps_by_table.items()
    ]

    missing_deps = {
        dep for dep_grp in missing_deps_by_table.values() for dep in dep_grp
    }
    job_names_seen_so_far = []
    for job in jobs:
        job_names_seen_so_far.append(job.job_name)
        for dep in job.dependencies:
            if dep not in job_names_seen_so_far and dep not in missing_deps:
                dep_issues.append(
                    f"{job.job_name.value} depends on job [{dep.value}] that comes after it in the "
                    f"list of jobs."
                )

    if dep_issues:
        return value_objects.Result.failure("\n".join(dep_issues))
    else:
        return value_objects.Result.success()


def _run_batch(
    batch_logger: batch_logging_service.BatchLoggingService,
    uow: unit_of_work.UnitOfWork,
    batch_id: value_objects.UniqueId,
    jobs: typing.Iterable[job_spec.JobSpec],
    ts_adapter: timestamp_adapter.TimestampAdapter,
) -> batch.Batch:
    start_ts = ts_adapter.now()

    job_results: List[job_result.JobResult] = []
    for job in jobs:
        with uow:
            last_ts = uow.batches.get_last_successful_ts_for_job(job_name=job.job_name)

        if last_ts:
            seconds_since_last_refresh = (
                uow.ts_adapter.now().value - last_ts.value
            ).total_seconds()
            if seconds_since_last_refresh < job.seconds_between_refreshes.value:
                batch_logger.log_info(
                    value_objects.LogMessage(
                        f"[{job.job_name.value}] was run successfully {seconds_since_last_refresh:.0f} seconds "
                        f"ago and it is set to refresh every {job.seconds_between_refreshes.value} seconds, "
                        f"so there is no need to refresh again."
                    )
                )
                continue

        job_id = value_objects.UniqueId.generate()
        job_logger = job_logging_service.DefaultJobLoggingService(
            uow=uow, batch_id=batch_id, job_id=job_id
        )
        result = job_runner.default_job_runner(
            uow=uow,
            job=job,
            logger=job_logger,
            batch_id=batch_id,
            job_id=job_id,
            ts_adapter=ts_adapter,
        )
        job_results.append(result)
        with uow:
            uow.batches.add_job_result(result)
            uow.commit()

    end_time = ts_adapter.now().value
    execution_millis = int((end_time - start_ts.value).total_seconds() * 1000)
    return batch.Batch(
        id=batch_id,
        execution_millis=value_objects.ExecutionMillis(execution_millis),
        job_results=frozenset(job_results),
        execution_success_or_failure=value_objects.Result.success(),
        running=value_objects.Flag(False),
        ts=uow.ts_adapter.now(),
    )
