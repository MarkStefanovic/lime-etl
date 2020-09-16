from __future__ import annotations

import datetime
import dataclasses

import typing

from domain import job_result, value_objects  # type: ignore


@dataclasses.dataclass(unsafe_hash=True)
class BatchDTO:
    id: str
    execution_error_message: typing.Optional[str]
    execution_error_occurred: typing.Optional[bool]
    execution_millis: typing.Optional[int]
    job_results: typing.List[job_result.JobResultDTO]
    running: bool
    ts: datetime.datetime

    def to_domain(self) -> Batch:
        results = frozenset(job.to_domain() for job in self.job_results)
        if self.execution_error_occurred:
            execution_success_or_failure = value_objects.Result.failure(
                self.execution_error_message or "No error message was provided."
            )
        else:
            execution_success_or_failure = value_objects.Result.success()

        return Batch(
            id=value_objects.UniqueId(self.id),
            execution_millis=value_objects.ExecutionMillis(self.execution_millis),
            job_results=results,
            execution_success_or_failure=execution_success_or_failure,
            running=value_objects.Flag(self.running),
            ts=value_objects.Timestamp(self.ts),
        )


@dataclasses.dataclass(frozen=True)
class Batch:
    id: value_objects.UniqueId
    job_results: typing.FrozenSet[job_result.JobResult]
    execution_success_or_failure: typing.Optional[value_objects.Result]
    execution_millis: typing.Optional[value_objects.ExecutionMillis]
    running: value_objects.Flag
    ts: value_objects.Timestamp

    @property
    def job_names(self) -> typing.Set[value_objects.JobName]:
        return {job.job_name for job in self.job_results}

    @property
    def broken_jobs(self) -> typing.Set[value_objects.JobName]:
        return {job.job_name for job in self.job_results if job.is_broken}

    def to_dto(self) -> BatchDTO:
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

        return BatchDTO(
            id=self.id.value,
            execution_millis=execution_millis,
            running=self.running.value,
            job_results=results,
            execution_error_occurred=error_occurred,
            execution_error_message=error_msg,
            ts=self.ts.value,
        )
