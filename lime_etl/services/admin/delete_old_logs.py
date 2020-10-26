import datetime
import typing

from lime_etl.domain import job_spec, job_test_result, value_objects
from lime_etl.services import admin_unit_of_work, job_logging_service


class DeleteOldLogs(job_spec.JobSpec):
    def __init__(
        self,
        admin_uow: admin_unit_of_work.AdminUnitOfWork,
        days_to_keep: value_objects.DaysToKeep,
        job_id: typing.Optional[value_objects.UniqueId] = None,
    ):
        self._admin_uow = admin_uow
        self._days_to_keep = days_to_keep
        super().__init__(
            dependencies=tuple(),
            job_id=job_id,
            job_name=value_objects.JobName("delete_old_logs"),
            max_retries=value_objects.MaxRetries(0),
            min_seconds_between_refreshes=value_objects.MinSecondsBetweenRefreshes(60 * 60 * 24),
            timeout_seconds=value_objects.TimeoutSeconds(300),
        )

    def on_execution_error(
        self, error_message: str
    ) -> typing.Optional[job_spec.JobSpec]:
        return None

    def on_test_failure(
        self, test_results: typing.FrozenSet[job_test_result.JobTestResult]
    ) -> typing.Optional[job_spec.JobSpec]:
        return None

    def run(
        self,
        /,
        logger: job_logging_service.AbstractJobLoggingService,
    ) -> value_objects.Result:
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

        return value_objects.Result.success()

    def test(
        self,
        /,
        logger: job_logging_service.AbstractJobLoggingService,
    ) -> typing.Collection[job_test_result.SimpleJobTestResult]:
        with self._admin_uow as uow:
            now = uow.ts_adapter.now().value
            cutoff_date = datetime.datetime.combine(
                (now - datetime.timedelta(days=self._days_to_keep.value)).date(),
                datetime.datetime.min.time(),
            )
            earliest_ts = uow.batch_log_repo.get_earliest_timestamp()

        if earliest_ts and earliest_ts < cutoff_date:
            return [
                job_test_result.SimpleJobTestResult(
                    test_name=value_objects.TestName(
                        "No log entries more than 3 days old"
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
                        "No log entries more than 3 days old"
                    ),
                    test_success_or_failure=value_objects.Result.success(),
                )
            ]
