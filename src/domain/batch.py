from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import FrozenSet, List, Optional, Set

from src.domain import value_objects
from src.domain.job_result import JobResult, JobResultDTO


@dataclass(unsafe_hash=True)
class BatchDTO:
    id: str
    execution_millis: int
    job_results: List[JobResultDTO]
    execution_error_occurred: bool
    execution_error_message: Optional[str]
    ts: datetime.datetime

    def to_domain(self) -> Batch:
        results = frozenset(job.to_domain() for job in self.job_results)
        if self.execution_error_occurred:
            execution_success_or_failure = value_objects.Result.failure(self.execution_error_message or "No error message was provided.")
        else:
            execution_success_or_failure = value_objects.Result.success()

        return Batch(
            id=value_objects.UniqueId(value=self.id),
            execution_millis=value_objects.ExecutionMillis(value=self.execution_millis),
            job_results=results,
            execution_success_or_failure=execution_success_or_failure,
            ts=value_objects.Timestamp(value=self.ts),
        )


@dataclass(frozen=True)
class Batch:
    id: value_objects.UniqueId
    job_results: FrozenSet[JobResult]
    execution_success_or_failure: value_objects.Result
    execution_millis: value_objects.ExecutionMillis
    ts: value_objects.Timestamp

    @property
    def job_names(self) -> Set[value_objects.JobName]:
        return {job.job_name for job in self.job_results}

    @property
    def broken_jobs(self) -> Set[value_objects.JobName]:
        return {job.job_name for job in self.job_results if job.is_broken}

    def to_dto(self) -> BatchDTO:
        results = [j.to_dto() for j in self.job_results]
        return BatchDTO(
            id=self.id.value,
            execution_millis=self.execution_millis.value,
            job_results=results,
            execution_error_occurred=self.execution_success_or_failure.is_failure,
            execution_error_message=self.execution_success_or_failure.failure_message_or_none,
            ts=self.ts.value,
        )
