import datetime
import typing

import lime_uow as lu

from lime_etl import domain

__all__ = ("DeleteOldLogs",)


class DeleteOldLogs(domain.JobSpec):
    def __init__(
        self,
        days_logs_to_keep: domain.DaysToKeep = domain.DaysToKeep(3),
        min_seconds_between_runs: domain.MinSecondsBetweenRefreshes = domain.MinSecondsBetweenRefreshes(
            0
        ),
    ):
        self._days_logs_to_keep = days_logs_to_keep
        self._min_seconds_between_runs = min_seconds_between_runs

    @property
    def job_name(self) -> domain.JobName:
        return domain.JobName("delete_old_logs")

    @property
    def min_seconds_between_refreshes(self) -> domain.MinSecondsBetweenRefreshes:
        return self._min_seconds_between_runs

    def run(
        self,
        uow: lu.UnitOfWork,
        logger: domain.JobLogger,
    ) -> domain.JobStatus:
        with uow:
            batch_log_repo = uow.get(domain.BatchLogRepository)  # type: ignore
            batch_log_repo.delete_old_entries(days_to_keep=self._days_logs_to_keep)
            logger.info(
                f"Deleted batch log entries older than {self._days_logs_to_keep.value} days old."
            )

            job_log_repo = uow.get(domain.JobLogRepository)  # type: ignore
            job_log_repo.delete_old_entries(days_to_keep=self._days_logs_to_keep)
            logger.info(
                f"Deleted job log entries older than {self._days_logs_to_keep.value} days old."
            )

            batch_repo = uow.get(domain.BatchRepository)  # type: ignore
            batch_repo.delete_old_entries(self._days_logs_to_keep)
            logger.info(
                f"Deleted batch results older than {self._days_logs_to_keep.value} days old."
            )
            uow.save()

        return domain.JobStatus.success()

    def test(
        self,
        uow: lu.UnitOfWork,
        logger: domain.JobLogger,
    ) -> typing.List[domain.SimpleJobTestResult]:
        with uow:
            now = uow.get(domain.TimestampAdapter).now().value  # type: ignore
            cutoff_date = datetime.datetime.combine(
                (now - datetime.timedelta(days=self._days_logs_to_keep.value)).date(),
                datetime.datetime.min.time(),
            )
            earliest_ts = uow.get(domain.BatchLogRepository).get_earliest_timestamp()  # type: ignore

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
