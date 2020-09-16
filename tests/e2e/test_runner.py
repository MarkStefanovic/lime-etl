import tempfile
from typing import Iterable, List

import sqlalchemy as sa

from domain import job_spec, job_test_result, value_objects  # type: ignore
from domain.job_spec import JobSpec  # type: ignore
from lime_etl import runner
from services import job_logging_service  # type: ignore


class HelloWorldJob(job_spec.ETLJobSpec):
    def run(self, logger: job_logging_service.JobLoggingService) -> None:
        with tempfile.TemporaryFile() as fp:
            fp.write(b"Hello World")

    @property
    def dependencies(self) -> List[JobSpec]:
        return []

    @property
    def flex_pct(self) -> value_objects.FlexPercent:
        return value_objects.FlexPercent(0)

    @property
    def job_name(self) -> value_objects.JobName:
        return value_objects.JobName("hello_world_job")

    @property
    def seconds_between_refreshes(self) -> value_objects.SecondsBetweenRefreshes:
        return value_objects.SecondsBetweenRefreshes(300)

    @property
    def timeout_seconds(self) -> value_objects.TimeoutSeconds:
        return value_objects.TimeoutSeconds(60)

    @property
    def max_retries(self) -> value_objects.MaxRetries:
        return value_objects.MaxRetries(1)

    def test(
        self, logger: job_logging_service.JobLoggingService
    ) -> Iterable[job_test_result.JobTestResult]:
        return []


def test_run_with_sqlite_using_default_parameters(in_memory_db: sa.engine.Engine) -> None:
    etl_jobs = [
        HelloWorldJob(),
    ]
    actual = runner.run(
        engine_or_uri=in_memory_db,
        etl_jobs=etl_jobs,
        admin_jobs=runner.DEFAULT_ADMIN_JOBS,
    )
    assert actual.current_results.job_names == {
        value_objects.JobName("hello_world_job"),
        value_objects.JobName("delete_old_logs"),
    }
    assert actual.current_results.broken_jobs == set()
    assert actual.current_results.running.value is False
    assert actual.current_results.execution_millis.value > 0
    assert actual.current_results.ts is not None
