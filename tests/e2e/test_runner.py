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
from lime_etl.services import job_logging_service
from lime_etl import runner


class HelloWorldJob(job_spec.ETLJobSpec):
    def __init__(self, job_name: str, dependencies: typing.List[value_objects.JobName]):
        self._job_name = job_name
        self._dependencies = dependencies
        self._file_created: bool = False

    def run(
        self,
        logger: job_logging_service.AbstractJobLoggingService,
        resources: typing.Mapping[value_objects.ResourceName, typing.Any],
    ) -> value_objects.Result:
        fh: typing.IO[bytes] = resources[value_objects.ResourceName("hello_world_file")]
        fh.write(b"Hello World")
        self._file_created = True
        return value_objects.Result.success()

    @property
    def dependencies(self) -> typing.List[value_objects.JobName]:
        return self._dependencies

    @property
    def job_name(self) -> value_objects.JobName:
        return value_objects.JobName(self._job_name)

    def on_execution_error(
        self, error_message: str
    ) -> typing.Optional[job_spec.ETLJobSpec]:
        return None

    def on_test_failure(
        self, test_results: typing.FrozenSet[job_test_result.JobTestResult]
    ) -> typing.Optional[job_spec.ETLJobSpec]:
        return None

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
        logger: job_logging_service.AbstractJobLoggingService,
        resources: typing.Mapping[
            value_objects.ResourceName, shared_resource.SharedResource[typing.IO[bytes]]
        ],
    ) -> typing.Collection[job_test_result.SimpleJobTestResult]:
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
        self._file_handle.close()  # type: ignore


def test_run_with_sqlite_using_default_parameters_happy_path(
    in_memory_db: sa.engine.Engine,
) -> None:
    etl_jobs = [
        HelloWorldJob("hello_world_job", dependencies=[]),
        HelloWorldJob(
            "hello_world_job2", dependencies=[value_objects.JobName("hello_world_job")]
        ),
    ]
    resources = [TempFileResource("hello_world_file")]
    actual = runner.run(
        batch_name="test_batch",
        engine_or_uri=in_memory_db,
        jobs=etl_jobs,
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
        value_objects.JobName("hello_world_job"),
        value_objects.JobName("hello_world_job2"),
    }
    assert actual.current_results.broken_jobs == set()
    assert actual.current_results.running.value is False
    assert actual.current_results.execution_millis is not None
    assert actual.current_results.execution_millis.value > 0
    assert actual.current_results.ts is not None

    sql = """
        SELECT execution_error_occurred, execution_error_message, ts 
        FROM batches 
        ORDER BY ts DESC
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    assert (
        len(result) == 1
    ), f"{len(result)} batches were added.  There should only be 1 batch entry."
    row = result[0]
    assert row["execution_error_occurred"] == 0
    assert row["execution_error_message"] is None
    assert row["ts"] is not None

    sql = """
        SELECT job_name, execution_millis, execution_error_occurred, execution_error_message, ts 
        FROM jobs 
        ORDER BY ts
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    assert (
        len(result) == 2
    ), f"{len(result)} job results were added, but 2 jobs were scheduled."
    job_names = {row["job_name"] for row in result}
    assert job_names == {"hello_world_job2", "hello_world_job"}
    assert all(row["execution_error_occurred"] == 0 for row in result)
    assert all(row["execution_error_message"] is None for row in result)
    assert all({row["ts"] is not None for row in result})

    sql = """
        SELECT batch_id, job_id, log_level, message, ts 
        FROM job_log 
        ORDER BY ts DESC
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    job_log_messages = {row["message"] for row in result}
    assert job_log_messages == {
        "Finished running [hello_world_job2].",
        "Finished running [hello_world_job].",
        "Running the tests for [hello_world_job2]...",
        "Running the tests for [hello_world_job]...",
        "Starting [hello_world_job2]...",
        "Starting [hello_world_job]...",
        "[hello_world_job2] finished successfully.",
        "[hello_world_job] finished successfully.",
        "hello_world_job test results: tests_passed=1, tests_failed=0",
        "hello_world_job2 test results: tests_passed=1, tests_failed=0",
    }


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
            batch_name="test_batch",
            engine_or_uri=in_memory_db,
            jobs=etl_jobs,
            resources=resources,
        )
    assert (
        str(e.value)
        == "[hello_world_job] has the following unresolved dependencies: [delete_old_logs2]."
    )

    sql = """
        SELECT execution_error_occurred, execution_error_message, ts 
        FROM batches 
        ORDER BY ts DESC
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    assert (
        len(result) == 1
    ), f"{len(result)} batches were added.  There should only be 1 batch entry."
    row = result[0]
    assert row["execution_error_occurred"] == 1
    assert (
        row["execution_error_message"]
        == "[hello_world_job] has the following unresolved dependencies: [delete_old_logs2]."
    )
    assert row["ts"] is not None

    sql = """
        SELECT job_name, execution_millis, execution_error_occurred, execution_error_message, ts 
        FROM jobs 
        ORDER BY ts
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    assert len(result) == 0, (
        f"Since the job spec is invalid, so the batch should have failed at initialization and no "
        f"jobs should have been run, but {len(result)} job entries were added to the jobs table."
    )
    sql = """
        SELECT batch_id, job_id, log_level, message, ts 
        FROM job_log 
        ORDER BY ts DESC
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    job_log_messages = {row["message"] for row in result}
    assert job_log_messages == set()


def test_run_with_dependencies_out_of_order(
    in_memory_db: sa.engine.Engine,
) -> None:  # sourcery skip: move-assign
    etl_jobs = [
        HelloWorldJob(
            "hello_world_job2", dependencies=[value_objects.JobName("hello_world_job")]
        ),  # job relies on dependency that comes after it
        HelloWorldJob("hello_world_job", dependencies=[]),
    ]
    resources = [TempFileResource("hello_world_file")]
    with pytest.raises(exceptions.DependencyErrors) as e:
        runner.run(
            batch_name="test_batch",
            engine_or_uri=in_memory_db,
            jobs=etl_jobs,
            resources=resources,
        )
    assert (
        str(e.value)
        == "[hello_world_job2] depends on the following jobs which come after it: [hello_world_job]."
    )

    sql = """
        SELECT job_name, execution_millis, execution_error_occurred, execution_error_message, ts 
        FROM jobs 
        ORDER BY ts
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    assert len(result) == 0, (
        f"Since the job spec is invalid, so the batch should have failed at initialization and no "
        f"jobs should have been run, but {len(result)} job entries were added to the jobs table."
    )
    sql = """
        SELECT batch_id, job_id, log_level, message, ts 
        FROM job_log 
        ORDER BY ts DESC
    """
    with in_memory_db.begin() as con:
        result = con.execute(sa.text(sql)).fetchall()
    job_log_messages = {row["message"] for row in result}
    assert job_log_messages == set()
