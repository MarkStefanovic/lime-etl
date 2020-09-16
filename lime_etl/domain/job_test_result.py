from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Optional


from domain import value_objects  # type: ignore


@dataclass(unsafe_hash=True)
class JobTestResultDTO:
    id: str
    job_id: str
    test_name: str
    test_passed: bool
    test_failure_message: Optional[str]
    ts: datetime.datetime

    def to_domain(self) -> JobTestResult:
        if self.test_passed:
            test_success_or_failure = value_objects.Result.success()
        else:
            test_success_or_failure = value_objects.Result.failure(
                self.test_failure_message or "No error message was provided."
            )

        return JobTestResult(
            id=value_objects.UniqueId(self.id),
            job_id=value_objects.UniqueId(self.job_id),
            test_name=value_objects.TestName(self.test_name),
            test_success_or_failure=test_success_or_failure,
            ts=value_objects.Timestamp(self.ts),
        )


@dataclass(frozen=True)
class JobTestResult:
    id: value_objects.UniqueId
    job_id: value_objects.UniqueId
    test_name: value_objects.TestName
    test_success_or_failure: value_objects.Result
    ts: value_objects.Timestamp

    @property
    def test_passed(self) -> bool:
        return not self.test_success_or_failure.is_failure

    def to_dto(self) -> JobTestResultDTO:
        return JobTestResultDTO(
            id=self.id.value,
            job_id=self.job_id.value,
            test_name=self.test_name.value,
            test_failure_message=self.test_success_or_failure.failure_message_or_none,
            test_passed=self.test_passed,
            ts=self.ts.value,
        )
