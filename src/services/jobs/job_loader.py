from typing import List

from src import settings
from src.domain import job_spec
from src.services.jobs import delete_old_logs


def load(project_settings: settings.Settings) -> List[job_spec.JobSpec]:
    return [
        delete_old_logs.DeleteOldLogs(days_to_keep=project_settings.days_of_logs_to_keep),
    ]
