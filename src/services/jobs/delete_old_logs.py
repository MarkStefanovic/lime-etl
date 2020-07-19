from typing import Iterable, List


from src.domain import job_test_result, value_objects
from src.services import job_logging_service, unit_of_work
from src.domain.job_spec import JobSpec


class DeleteOldLogs(JobSpec):
    def __init__(
        self,
        days_to_keep: value_objects.DaysToKeep,
    ):
        self._days_to_keep = days_to_keep

    @property
    def dependencies(self) -> List[JobSpec]:
        return []

    @property
    def flex_pct(self) -> value_objects.FlexPercent:
        return value_objects.FlexPercent(value=0)

    @property
    def seconds_between_refreshes(self) -> value_objects.SecondsBetweenRefreshes:
        return value_objects.SecondsBetweenRefreshes(value=60 * 60 * 24)

    @property
    def timeout_seconds(self) -> value_objects.TimeoutSeconds:
        return value_objects.TimeoutSeconds(value=300)

    @property
    def max_retries(self) -> value_objects.MaxRetries:
        return value_objects.MaxRetries(value=1)

    def run(
        self,
        uow: unit_of_work.UnitOfWork,
        logger: job_logging_service.JobLoggingService,
    ) -> None:
        with uow:
            uow.batch_log.delete_old_entries(
                days_to_keep=self._days_to_keep
            )
            logger.log_info(
                uow=uow,
                message=value_objects.LogMessage(
                    f"Deleted batch log entries older than {self._days_to_keep.value} days old."
                ),
            )
            uow.batch_log.delete_old_entries(
                days_to_keep=self._days_to_keep
            )
            logger.log_info(
                uow=uow,
                message=value_objects.LogMessage(
                    f"Deleted job log entries older than {self._days_to_keep.value} days old."
                ),
            )

            uow.commit()
            return None

    def test(
        self,
        uow: unit_of_work.UnitOfWork,
        logger: job_logging_service.JobLoggingService,
    ) -> Iterable[job_test_result.JobTestResult]:
        # TODO implement
        return []
