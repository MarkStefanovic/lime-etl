from __future__ import annotations

import dataclasses
import datetime
import typing

from lime_etl.domain import value_objects


@dataclasses.dataclass(unsafe_hash=True)
class JobTestResultDTO:
    id: str
    job_id: str
    test_name: str
    test_passed: bool
    test_failure_message: typing.Optional[str]
    execution_millis: int
    execution_error_occurred: bool
    execution_error_message: typing.Optional[str]
    ts: datetime.datetime

    def to_domain(self) -> JobTestResult:
        if self.test_passed:
            test_success_or_failure = value_objects.Result.success()
        else:
            test_success_or_failure = value_objects.Result.failure(
                self.test_failure_message or "No error message was provided."
            )

        if self.execution_error_occurred:
            execution_success_or_failure = value_objects.Result.failure(
                self.execution_error_message or "No error message was provided."
            )
        else:
            execution_success_or_failure = value_objects.Result.success()

        return JobTestResult(
            id=value_objects.UniqueId(self.id),
            job_id=value_objects.UniqueId(self.job_id),
            test_name=value_objects.TestName(self.test_name),
            test_success_or_failure=test_success_or_failure,
            execution_millis=value_objects.ExecutionMillis(self.execution_millis),
            execution_success_or_failure=execution_success_or_failure,
            ts=value_objects.Timestamp(self.ts),
        )


@dataclasses.dataclass(frozen=True)
class JobTestResult:
    id: value_objects.UniqueId
    job_id: value_objects.UniqueId
    test_name: value_objects.TestName
    test_success_or_failure: value_objects.Result
    execution_millis: value_objects.ExecutionMillis
    execution_success_or_failure: value_objects.Result
    ts: value_objects.Timestamp

    @property
    def test_failed(self) -> bool:
        return self.test_success_or_failure.is_failure

    @property
    def test_passed(self) -> bool:
        return not self.test_failed

    def to_dto(self) -> JobTestResultDTO:
        return JobTestResultDTO(
            id=self.id.value,
            job_id=self.job_id.value,
            test_name=self.test_name.value,
            test_failure_message=self.test_success_or_failure.failure_message_or_none,
            test_passed=self.test_passed,
            execution_millis=self.execution_millis.value,
            execution_error_occurred=self.execution_success_or_failure.is_failure,
            execution_error_message=self.execution_success_or_failure.failure_message_or_none,
            ts=self.ts.value,
        )


@dataclasses.dataclass(frozen=True)
class SimpleJobTestResult:
    test_name: value_objects.TestName
    test_success_or_failure: value_objects.Result

    @property
    def test_failed(self) -> bool:
        return self.test_success_or_failure.is_failure

    @property
    def test_passed(self) -> bool:
        return self.test_success_or_failure.is_success
