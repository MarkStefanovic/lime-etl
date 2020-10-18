import datetime
import typing
from typing import List

from domain import batch_log_entry
from lime_etl.adapters import (
    timestamp_adapter,
)
from lime_etl.domain import job_spec, job_test_result, value_objects
from lime_etl.services import admin_unit_of_work, job_logging_service


class DeleteOldLogs(job_spec.AdminJobSpec):
    def __init__(
        self,
        days_to_keep: value_objects.DaysToKeep,
        ts_adapter: timestamp_adapter.TimestampAdapter = timestamp_adapter.LocalTimestampAdapter(),
    ):
        self._days_to_keep = days_to_keep
        self._ts_adapter = ts_adapter

    @property
    def dependencies(self) -> List[value_objects.JobName]:
        return []

    @property
    def max_retries(self) -> value_objects.MaxRetries:
        return value_objects.MaxRetries(1)

    def on_execution_error(
        self, error_message: str
    ) -> typing.Optional[job_spec.AdminJobSpec]:
        return None

    def on_test_failure(
        self, test_results: typing.FrozenSet[job_test_result.JobTestResult]
    ) -> typing.Optional[job_spec.AdminJobSpec]:
        return None

    @property
    def job_name(self) -> value_objects.JobName:
        return value_objects.JobName("delete_old_logs")

    @property
    def seconds_between_refreshes(self) -> value_objects.SecondsBetweenRefreshes:
        return value_objects.SecondsBetweenRefreshes(60 * 60 * 24)

    @property
    def timeout_seconds(self) -> value_objects.TimeoutSeconds:
        return value_objects.TimeoutSeconds(300)

    def run(
        self,
        admin_uow: admin_unit_of_work.AdminUnitOfWork,
        logger: job_logging_service.AbstractJobLoggingService,
    ) -> value_objects.Result:
        with admin_uow as uow:
            uow.batch_log_repo.delete_old_entries(days_to_keep=self._days_to_keep)
            logger.log_info(
                f"Deleted batch log entries older than {self._days_to_keep.value} days old."
            )

            uow.job_log_repo.delete_old_entries(days_to_keep=self._days_to_keep)
            uow.save()
            logger.log_info(
                f"Deleted job log entries older than {self._days_to_keep.value} days old."
            )

            uow.batch_repo.delete_old_entries(self._days_to_keep)
            admin_uow.save()
            logger.log_info(
                f"Deleted batch results older than {self._days_to_keep.value} days old."
            )

        return value_objects.Result.success()

    def test(
        self,
        admin_uow: admin_unit_of_work.AdminUnitOfWork,
        logger: job_logging_service.AbstractJobLoggingService,
    ) -> typing.Collection[job_test_result.SimpleJobTestResult]:
        cutoff_date = datetime.datetime.combine(
            (
                self._ts_adapter.now().value
                - datetime.timedelta(days=self._days_to_keep.value)
            ).date(),
            datetime.datetime.min.time(),
        )
        with admin_uow as uow:
            earliest_ts = uow.batch_log_repo.get_earliest_timestamp()

        if earliest_ts and earliest_ts < cutoff_date:
            return [
                job_test_result.SimpleJobTestResult(
                    test_name=value_objects.TestName(
                        "No log entries more than than 3 days old"
                    ),
                    test_success_or_failure=value_objects.Result.failure(
                        f"The earliest batch log entry is from "
                        f"{earliest_ts.strftime('%Y-%m-%d %H:%M:%S')}"
                    ),
                )
            ]
        else:
            return [
                job_test_result.SimpleJobTestResult(
                    test_name=value_objects.TestName(
                        "No log entries more than than 3 days old"
                    ),
                    test_success_or_failure=value_objects.Result.success(),
                )
            ]
