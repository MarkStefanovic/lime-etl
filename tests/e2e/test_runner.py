import tempfile

import pytest
import sqlalchemy as sa
import typing

from lime_etl.domain import (
    exceptions,
    job_spec,
    job_test_result,
    shared_resource,
    value_objects,
)
from lime_etl import runner
from lime_etl.services import job_logging_service


class HelloWorldJob(job_spec.ETLJobSpec):
    def __init__(self, job_name: str, dependencies: typing.List[value_objects.JobName]):
        self._job_name = job_name
        self._dependencies = dependencies
        self._file_created: bool = False

    def run(
        self,
        logger: job_logging_service.JobLoggingService,
        resources: typing.Mapping[value_objects.ResourceName, typing.Any],
    ) -> None:
        fh: typing.IO[bytes] = resources[value_objects.ResourceName("hello_world_file")]
        fh.write(b"Hello World")
        self._file_created = True

    @property
    def dependencies(self) -> typing.List[value_objects.JobName]:
        return self._dependencies

    @property
    def flex_pct(self) -> value_objects.FlexPercent:
        return value_objects.FlexPercent(0)

    @property
    def job_name(self) -> value_objects.JobName:
        return value_objects.JobName(self._job_name)

    @property
    def seconds_between_refreshes(self) -> value_objects.SecondsBetweenRefreshes:
        return value_objects.SecondsBetweenRefreshes(300)

    @property
    def timeout_seconds(self) -> value_objects.TimeoutSeconds:
        return value_objects.TimeoutSeconds(60)

    @property
    def max_retries(self) -> value_objects.MaxRetries:
        return value_objects.MaxRetries(1)

    @property
    def resources_needed(
        self,
    ) -> typing.Collection[value_objects.ResourceName]:
        return [value_objects.ResourceName("hello_world_file")]

    def test(
        self,
        logger: job_logging_service.JobLoggingService,
        resources: typing.Mapping[
            value_objects.ResourceName, shared_resource.SharedResource
        ],
    ) -> typing.Iterable[job_test_result.SimpleJobTestResult]:
        if self._file_created:
            return [
                job_test_result.SimpleJobTestResult(
                    test_name=value_objects.TestName(f"test_{self._job_name}"),
                    test_success_or_failure=value_objects.Result.success(),
                )
            ]
        else:
            return [
                job_test_result.SimpleJobTestResult(
                    test_name=value_objects.TestName(f"test_{self._job_name}"),
                    test_success_or_failure=value_objects.Result.failure("Failed"),
                )
            ]


class TempFileResource(shared_resource.SharedResource[typing.IO[bytes]]):
    def __init__(self, name: str):
        self._name = name
        self._file_handle: typing.Optional[typing.IO[bytes]] = None

    @property
    def name(self) -> value_objects.ResourceName:
        return value_objects.ResourceName(self._name)

    def open(self) -> typing.IO[bytes]:
        self._file_handle = tempfile.TemporaryFile()
        return self._file_handle

    def close(self) -> None:
        self._file_handle.close()


def test_run_with_sqlite_using_default_parameters(
    in_memory_db: sa.engine.Engine,
) -> None:
    etl_jobs = [
        HelloWorldJob(
            "hello_world_job", dependencies=[value_objects.JobName("delete_old_logs")]
        ),
        HelloWorldJob(
            "hello_world_job2", dependencies=[value_objects.JobName("delete_old_logs")]
        ),
    ]
    resources = [TempFileResource("hello_world_file")]
    actual = runner.run(
        admin_jobs=runner.DEFAULT_ADMIN_JOBS,
        engine_or_uri=in_memory_db,
        etl_jobs=etl_jobs,
        resources=resources,
    )

    job_execution_results = [
        jr.execution_success_or_failure for jr in actual.current_results.job_results
    ]
    assert all(
        jr.execution_success_or_failure == value_objects.Result.success()
        for jr in actual.current_results.job_results
    ), f"Expected all Success values, but got {job_execution_results}"

    assert actual.current_results.job_names == {
        value_objects.JobName("delete_old_logs"),
        value_objects.JobName("hello_world_job"),
        value_objects.JobName("hello_world_job2"),
    }

    assert actual.current_results.broken_jobs == set(), [
        j.execution_success_or_failure.value for j in actual.current_results.job_results
    ]

    assert actual.current_results.running.value is False

    assert actual.current_results.execution_millis.value > 0

    assert actual.current_results.ts is not None


def test_run_with_unresolved_dependencies(
    in_memory_db: sa.engine.Engine,
) -> None:
    etl_jobs = [
        HelloWorldJob(
            "hello_world_job", dependencies=[value_objects.JobName("delete_old_logs2")]
        ),  # dependency does not exist
        HelloWorldJob(
            "hello_world_job2", dependencies=[value_objects.JobName("hello_world_job")]
        ),
    ]
    resources = [TempFileResource("hello_world_file")]
    with pytest.raises(exceptions.DependencyErrors) as e:
        runner.run(
            admin_jobs=runner.DEFAULT_ADMIN_JOBS,
            engine_or_uri=in_memory_db,
            etl_jobs=etl_jobs,
            resources=resources,
        )

    assert (
        str(e.value)
        == "[hello_world_job] has the following unresolved dependencies: [delete_old_logs2]."
    )


def test_run_with_dependencies_out_of_order(
    in_memory_db: sa.engine.Engine,
) -> None:
    etl_jobs = [
        HelloWorldJob(
            "hello_world_job2", dependencies=[value_objects.JobName("hello_world_job")]
        ),  # job relies on dependency that comes after it
        HelloWorldJob("hello_world_job", dependencies=[]),
    ]
    resources = [TempFileResource("hello_world_file")]
    with pytest.raises(exceptions.DependencyErrors) as e:
        runner.run(
            engine_or_uri=in_memory_db,
            etl_jobs=etl_jobs,
            admin_jobs=runner.DEFAULT_ADMIN_JOBS,
            resources=resources,
        )

    assert (
        str(e.value)
        == "[hello_world_job2] depends on the following jobs which come after it: [hello_world_job]."
    )
