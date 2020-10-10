from __future__ import annotations

import datetime
import typing
from dataclasses import dataclass
from typing import FrozenSet, List, Optional

from lime_etl.domain import exceptions, job_test_result, value_objects


@dataclass(unsafe_hash=True)
class JobResultDTO:
    id: str
    batch_id: str
    job_name: str
    test_results: List[job_test_result.JobTestResultDTO]
    execution_millis: typing.Optional[int]
    execution_error_occurred:typing.Optional[bool]
    execution_error_message: typing.Optional[str]
    running: bool
    ts: datetime.datetime

    def to_domain(self) -> JobResult:
        test_results = frozenset(dto.to_domain() for dto in self.test_results)
        if self.running:
            execution_millis = None
            execution_success_or_failure = None
        else:
            execution_millis = value_objects.ExecutionMillis(self.execution_millis or 0)
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
            execution_millis=execution_millis,
            execution_success_or_failure=execution_success_or_failure,
            running=value_objects.Flag(self.running),
            ts=value_objects.Timestamp(self.ts),
        )


@dataclass(frozen=True)
class JobResult:
    id: value_objects.UniqueId
    batch_id: value_objects.UniqueId
    job_name: value_objects.JobName
    test_results: FrozenSet[job_test_result.JobTestResult]
    execution_millis: typing.Optional[value_objects.ExecutionMillis]
    execution_success_or_failure: typing.Optional[value_objects.Result]
    running: value_objects.Flag
    ts: value_objects.Timestamp

    def __post_init__(self) -> None:
        if self.running.value is True:
            if self.execution_success_or_failure:
                raise exceptions.InvalidJobResult(
                    f"If a job is still running, execution_success_or_failure should be None, "
                    f"but got {self.execution_success_or_failure!r}."
                )
            if self.execution_millis:
                raise exceptions.InvalidJobResult(
                    f"If a job is running, execution_millis should be None, but got "
                    f"{self.execution_millis!r}."
                )
        else:
            if self.execution_success_or_failure is None:
                raise exceptions.InvalidJobResult(
                    "If a job has finished, then we should know the result, but "
                    "execution_success_or_failure is None."
                )
            if self.execution_millis is None:
                raise exceptions.InvalidJobResult(
                    "If a job has finished, then we should know how many milliseconds it took to "
                    "run, but execution_millis is None."
                )

    @property
    def tests_failed(self) -> bool:
        return any(result.test_failed for result in self.test_results)

    def to_dto(self) -> JobResultDTO:
        test_results = [r.to_dto() for r in self.test_results]

        if self.execution_success_or_failure is None:
            error_occurred = None
            error_msg = None
        else:
            error_occurred = self.execution_success_or_failure.is_failure
            error_msg = self.execution_success_or_failure.failure_message_or_none

        if self.execution_millis is None:
            execution_millis = None
        else:
            execution_millis = self.execution_millis.value

        return JobResultDTO(
            id=self.id.value,
            batch_id=self.batch_id.value,
            job_name=self.job_name.value,
            test_results=test_results,
            execution_millis=execution_millis,
            execution_error_occurred=error_occurred,
            execution_error_message=error_msg,
            running=self.running.value,
            ts=self.ts.value,
        )
