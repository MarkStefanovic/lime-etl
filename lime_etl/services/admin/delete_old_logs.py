from typing import Iterable, List

from domain import job_spec, job_test_result, value_objects  # type: ignore
from services import job_logging_service, unit_of_work  # type: ignore


class DeleteOldLogs(job_spec.AdminJobSpec):
    def __init__(
        self,
        days_to_keep: value_objects.DaysToKeep,
    ):
        self._days_to_keep = days_to_keep

    @property
    def dependencies(self) -> List[job_spec.JobSpec]:
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
        uow: unit_of_work,
        logger: job_logging_service.JobLoggingService,
    ) -> None:
        with uow:
            uow.batch_log.delete_old_entries(
                days_to_keep=self._days_to_keep
            )
            logger.log_info(
                message=value_objects.LogMessage(
                    f"Deleted batch log entries older than {self._days_to_keep.value} days old."
                ),
            )
            uow.batch_log.delete_old_entries(
                days_to_keep=self._days_to_keep
            )
            logger.log_info(
                message=value_objects.LogMessage(
                    f"Deleted job log entries older than {self._days_to_keep.value} days old."
                ),
            )

            uow.commit()
            return None

    def test(
        self,
        logger: job_logging_service.JobLoggingService,
    ) -> Iterable[job_test_result.JobTestResult]:
        # TODO implement
        return []
