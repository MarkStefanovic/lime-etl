from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import FrozenSet, List, Optional

from domain import job_test_result, value_objects


@dataclass(unsafe_hash=True)
class JobResultDTO:
    id: str
    batch_id: str
    job_name: str
    test_results: List[job_test_result.JobTestResultDTO]
    execution_millis: int
    execution_error_occurred: bool
    execution_error_message: Optional[str]
    ts: datetime.datetime

    def to_domain(self) -> JobResult:
        test_results = frozenset(dto.to_domain() for dto in self.test_results)
        if self.execution_error_occurred:
            execution_success_or_failure = value_objects.Result.failure(
                self.execution_error_message or "No error message was provided."
            )
        else:
            execution_success_or_failure = value_objects.Result.success()

        return JobResult(
            id=value_objects.UniqueId(self.id),
            batch_id=value_objects.UniqueId(self.batch_id),
            job_name=value_objects.JobName(self.job_name),
            test_results=test_results,
            execution_millis=value_objects.ExecutionMillis(self.execution_millis),
            execution_success_or_failure=execution_success_or_failure,
            ts=value_objects.Timestamp(self.ts),
        )


@dataclass(frozen=True)
class JobResult:
    id: value_objects.UniqueId
    batch_id: value_objects.UniqueId
    job_name: value_objects.JobName
    test_results: FrozenSet[job_test_result.JobTestResult]
    execution_millis: value_objects.ExecutionMillis
    execution_success_or_failure: value_objects.Result
    ts: value_objects.Timestamp

    @property
    def is_broken(self) -> bool:
        return any(not result.test_passed for result in self.test_results)

    def to_dto(self) -> JobResultDTO:
        test_results = [r.to_dto() for r in self.test_results]
        return JobResultDTO(
            id=self.id.value,
            batch_id=self.batch_id.value,
            job_name=self.job_name.value,
            test_results=test_results,
            execution_millis=self.execution_millis.value,
            execution_error_occurred=self.execution_success_or_failure.is_failure,
            execution_error_message=self.execution_success_or_failure.failure_message_or_none,
            ts=self.ts.value,
        )
