from __future__ import annotations

import datetime
import dataclasses

import typing

from lime_etl.domain import exceptions, job_result, value_objects


__all__ = (
    "BatchStatusDTO",
    "BatchStatus",
)


@dataclasses.dataclass(unsafe_hash=True)
class BatchStatusDTO:
    id: str
    name: str
    execution_error_message: typing.Optional[str]
    execution_error_occurred: typing.Optional[bool]
    execution_millis: typing.Optional[int]
    job_results: typing.List[job_result.JobResultDTO]
    running: bool
    ts: datetime.datetime

    def to_domain(self) -> BatchStatus:
        results = frozenset(job.to_domain() for job in self.job_results)
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

        return BatchStatus(
            id=value_objects.UniqueId(self.id),
            name=value_objects.BatchName(self.name),
            execution_millis=execution_millis,
            job_results=results,
            execution_success_or_failure=execution_success_or_failure,
            running=value_objects.Flag(self.running),
            ts=value_objects.Timestamp(self.ts),
        )


@dataclasses.dataclass(frozen=True)
class BatchStatus:
    id: value_objects.UniqueId
    name: value_objects.BatchName
    job_results: typing.FrozenSet[job_result.JobResult]
    execution_success_or_failure: typing.Optional[value_objects.Result]
    execution_millis: typing.Optional[value_objects.ExecutionMillis]
    running: value_objects.Flag
    ts: value_objects.Timestamp

    def __post_init__(self) -> None:
        if self.running.value is True:
            if self.execution_success_or_failure:
                raise exceptions.InvalidBatch(
                    f"If a batch is still running, execution_success_or_failure should be None, "
                    f"but got {self.execution_success_or_failure!r}."
                )
            if self.execution_millis:
                raise exceptions.InvalidBatch(
                    f"If a batch is running, execution_millis should be None, but got "
                    f"{self.execution_millis!r}."
                )
        else:
            if self.execution_success_or_failure is None:
                raise exceptions.InvalidBatch(
                    "If a bach has finished, then we should know the result, but "
                    "execution_success_or_failure is None."
                )
            if self.execution_millis is None:
                raise exceptions.InvalidBatch(
                    "If a batch has finished, then we should know how many milliseconds it took to "
                    "run, but execution_millis is None."
                )

    @property
    def job_names(self) -> typing.Set[value_objects.JobName]:
        return {job.job_name for job in self.job_results}

    @property
    def broken_jobs(self) -> typing.Set[value_objects.JobName]:
        return {job.job_name for job in self.job_results if job.tests_failed}

    def to_dto(self) -> BatchStatusDTO:
        results = [j.to_dto() for j in self.job_results]
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

        return BatchStatusDTO(
            id=self.id.value,
            name=self.name.value,
            execution_millis=execution_millis,
            running=self.running.value,
            job_results=results,
            execution_error_occurred=error_occurred,
            execution_error_message=error_msg,
            ts=self.ts.value,
        )
