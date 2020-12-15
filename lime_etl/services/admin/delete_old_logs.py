import datetime
import typing

from lime_etl import domain
from lime_etl.services import admin_unit_of_work, job_spec

__all__ = ("DeleteOldLogs",)


class DeleteOldLogs(job_spec.JobSpec[admin_unit_of_work.AdminUnitOfWork]):
    def __init__(self, days_logs_to_keep: domain.DaysToKeep = domain.DaysToKeep(3)):
        self._days_logs_to_keep = days_logs_to_keep

    @property
    def job_name(self) -> domain.JobName:
        return domain.JobName("delete_old_logs")

    def run(
        self,
        uow: admin_unit_of_work.AdminUnitOfWork,
        logger: domain.JobLoggingService,
    ) -> domain.JobStatus:
        with uow:
            uow.batch_log_repo.delete_old_entries(days_to_keep=self._days_logs_to_keep)
            logger.log_info(
                f"Deleted batch log entries older than {self._days_logs_to_keep.value} days old."
            )

            uow.job_log_repo.delete_old_entries(days_to_keep=self._days_logs_to_keep)
            logger.log_info(
                f"Deleted job log entries older than {self._days_logs_to_keep.value} days old."
            )

            uow.batch_repo.delete_old_entries(self._days_logs_to_keep)
            logger.log_info(
                f"Deleted batch results older than {self._days_logs_to_keep.value} days old."
            )
            uow.save()

        return domain.JobStatus.success()

    def test(
        self,
        uow: admin_unit_of_work.AdminUnitOfWork,
        logger: domain.JobLoggingService,
    ) -> typing.Collection[domain.SimpleJobTestResult]:
        with uow:
            now = uow.ts_adapter.now().value
            cutoff_date = datetime.datetime.combine(
                (now - datetime.timedelta(days=self._days_logs_to_keep.value)).date(),
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
