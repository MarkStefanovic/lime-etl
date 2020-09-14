from dataclasses import dataclass
from typing import FrozenSet, Set

from domain import batch, value_objects


@dataclass(frozen=True)
class BatchDelta:
    current_results: batch.Batch
    previous_results: batch.Batch

    @property
    def common_jobs(self) -> Set[value_objects.JobName]:
        return self.previous_results.job_names & self.current_results.job_names

    @property
    def newly_broken_jobs(self) -> Set[value_objects.JobName]:
        return self.current_results.broken_jobs - self.previous_results.broken_jobs

    @property
    def newly_fixed_jobs(self) -> Set[value_objects.JobName]:
        return self.previous_results.broken_jobs - self.current_results.broken_jobs

    def __str__(self):
        return (
            "The following jobs are broken:\n    " + "\n    ".join(f"{j}\n    " for j in self.current_results.broken_jobs),
            "The following jobs are newly broken:\n    " + "\n    ".join(f"{j}\n    " for j in self.newly_broken_jobs),
            "The following jobs have been fixed:\n    " + "\n    ".join(f"{j}\n    " for j in self.newly_fixed_jobs),
        )
