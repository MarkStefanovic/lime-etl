import datetime
import typing

import lime_uow as lu

from lime_etl import domain
from lime_etl.services import admin_unit_of_work, job_logging_service, job_spec

__all__ = ("DeleteOldLogs",)


class DeleteOldLogs(job_spec.JobSpec):
    def __init__(
        self,
        admin_uow: admin_unit_of_work.AdminUnitOfWork,
        days_to_keep: domain.DaysToKeep,
        job_id: typing.Optional[domain.UniqueId] = None,
    ):
        self._admin_uow = admin_uow
        self._days_to_keep = days_to_keep
        super().__init__(
            dependencies=tuple(),
            job_id=job_id,
            job_name=domain.JobName("delete_old_logs"),
            max_retries=domain.MaxRetries(0),
            min_seconds_between_refreshes=domain.MinSecondsBetweenRefreshes(
                60 * 60 * 24
            ),
            timeout_seconds=domain.TimeoutSeconds(300),
        )

    def on_execution_error(self, error_message: str) -> typing.Optional[job_spec.JobSpec]:
        return None

    def on_test_failure(
        self, test_results: typing.FrozenSet[domain.JobTestResult]
    ) -> typing.Optional[job_spec.JobSpec]:
        return None

    def run(
        self,
        uow: lu.UnitOfWork,
        logger: job_logging_service.AbstractJobLoggingService,
    ) -> domain.Result:
        with self._admin_uow as uow:
            uow.batch_log_repo.delete_old_entries(days_to_keep=self._days_to_keep)
            logger.log_info(
                f"Deleted batch log entries older than {self._days_to_keep.value} days old."
            )

            uow.job_log_repo.delete_old_entries(days_to_keep=self._days_to_keep)
            logger.log_info(
                f"Deleted job log entries older than {self._days_to_keep.value} days old."
            )

            uow.batch_repo.delete_old_entries(self._days_to_keep)
            logger.log_info(
                f"Deleted batch results older than {self._days_to_keep.value} days old."
            )
            uow.save()

        return domain.Result.success()

    def test(
        self,
        uow: lu.UnitOfWork,
        logger: job_logging_service.AbstractJobLoggingService,
    ) -> typing.Collection[domain.SimpleJobTestResult]:
        with self._admin_uow as uow:
            now = uow.ts_adapter.now().value
            cutoff_date = datetime.datetime.combine(
                (now - datetime.timedelta(days=self._days_to_keep.value)).date(),
                datetime.datetime.min.time(),
            )
            earliest_ts = uow.batch_log_repo.get_earliest_timestamp()

        if earliest_ts and earliest_ts < cutoff_date:
            return [
                domain.SimpleJobTestResult(
                    test_name=domain.TestName("No log entries more than 3 days old"),
                    test_success_or_failure=domain.Result.failure(
                        f"The earliest batch log entry is from "
                        f"{earliest_ts.strftime('%Y-%m-%d %H:%M:%S')}"
                    ),
                )
            ]
        else:
            return [
                domain.SimpleJobTestResult(
                    test_name=domain.TestName("No log entries more than 3 days old"),
                    test_success_or_failure=domain.Result.success(),
                )
            ]
