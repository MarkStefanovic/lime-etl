import abc

import lime_uow as lu

from lime_etl.domain import job_log_entry, value_objects

__all__ = (
    "JobLogRepository",
)


class JobLogRepository(
    lu.Repository[job_log_entry.JobLogEntryDTO],
    abc.ABC,
):
    @staticmethod
    def key() -> str:
        return JobLogRepository.__name__

    @abc.abstractmethod
    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        raise NotImplementedError


