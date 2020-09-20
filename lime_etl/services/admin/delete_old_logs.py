import datetime
import typing
from typing import List

from lime_etl.domain import job_spec, job_test_result, value_objects
from lime_etl.services import job_logging_service, unit_of_work


class DeleteOldLogs(job_spec.AdminJobSpec):
    def __init__(
        self,
        days_to_keep: value_objects.DaysToKeep,
    ):
        self._days_to_keep = days_to_keep

    @property
    def dependencies(self) -> List[value_objects.JobName]:
        return []

    @property
    def flex_pct(self) -> value_objects.FlexPercent:
        return value_objects.FlexPercent(0)

    @property
    def max_retries(self) -> value_objects.MaxRetries:
        return value_objects.MaxRetries(1)

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
        uow: unit_of_work.UnitOfWork,
        logger: job_logging_service.JobLoggingService,
    ) -> value_objects.Result:
        with uow:
            uow.batch_log.delete_old_entries(days_to_keep=self._days_to_keep)
            uow.commit()

        logger.log_info(
            message=value_objects.LogMessage(
                f"Deleted batch log entries older than {self._days_to_keep.value} days old."
            )
        )

        with uow:
            uow.job_log.delete_old_entries(days_to_keep=self._days_to_keep)
            uow.commit()

        logger.log_info(
            message=value_objects.LogMessage(
                f"Deleted job log entries older than {self._days_to_keep.value} days old."
            )
        )

        with uow:
            uow.batches.delete_old_entries(days_to_keep=self._days_to_keep)
            uow.commit()

        logger.log_info(
            message=value_objects.LogMessage(
                f"Deleted batch results older than {self._days_to_keep.value} days old."
            )
        )

        return value_objects.Result.success()

    def test(
        self,
        uow: unit_of_work.UnitOfWork,
        logger: job_logging_service.JobLoggingService,
    ) -> typing.Collection[job_test_result.SimpleJobTestResult]:
        with uow:
            ts = uow.ts_adapter.now().value
            cutoff_date = datetime.datetime.combine(
                (ts - datetime.timedelta(days=self._days_to_keep.value)).date(),
                datetime.datetime.min.time(),
            )
            earliest_log_entry = uow.batch_log.get_earliest()

        if earliest_log_entry.ts.value < cutoff_date:
            return [
                job_test_result.SimpleJobTestResult(
                    test_name=value_objects.TestName(
                        "No log entries more than than 3 days old"
                    ),
                    test_success_or_failure=value_objects.Result.failure(
                        f"The earliest batch log entry is from "
                        f"{earliest_log_entry.ts.value.strftime('%Y-%m-%d %H:%M:%S')}"
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
