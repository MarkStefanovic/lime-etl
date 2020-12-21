from __future__ import annotations

import dataclasses
import datetime
import typing

from lime_etl.domain import job_status, job_test_result, value_objects

__all__ = (
    "JobResultDTO",
    "JobResult",
)


@dataclasses.dataclass(unsafe_hash=True)
class JobResultDTO:
    id: str
    batch_id: str
    job_name: str
    test_results: typing.List[job_test_result.JobTestResultDTO]
    execution_millis: typing.Optional[int]
    execution_error_occurred: typing.Optional[bool]
    execution_error_message: typing.Optional[str]
    running: bool
    skipped: bool
    ts: datetime.datetime

    def to_domain(self) -> JobResult:
        test_results = frozenset(dto.to_domain() for dto in self.test_results)
        if self.running:
            execution_millis = None
            status: job_status.JobStatus = job_status.JobStatus.in_progress()
        else:
            execution_millis = value_objects.ExecutionMillis(self.execution_millis or 0)
            if self.execution_error_occurred:
                status = job_status.JobStatus.failed(
                    self.execution_error_message or "No error message was provided."
                )
            else:
                status = job_status.JobStatus.success()

        return JobResult(
            id=value_objects.UniqueId(self.id),
            batch_id=value_objects.UniqueId(self.batch_id),
            job_name=value_objects.JobName(self.job_name),
            test_results=test_results,
            execution_millis=execution_millis,
            status=status,
            ts=value_objects.Timestamp(self.ts),
        )


@dataclasses.dataclass(frozen=True)
class JobResult:
    batch_id: value_objects.UniqueId
    execution_millis: typing.Optional[value_objects.ExecutionMillis]
    status: job_status.JobStatus
    id: value_objects.UniqueId
    job_name: value_objects.JobName
    ts: value_objects.Timestamp
    test_results: typing.FrozenSet[job_test_result.JobTestResult]

    @property
    def tests_failed(self) -> bool:
        return any(result.test_failed for result in self.test_results)

    def to_dto(self) -> JobResultDTO:
        test_results = [r.to_dto() for r in self.test_results]

        if isinstance(self.status, job_status.JobFailed):
            error_occurred: typing.Optional[bool] = True
            error_message: typing.Optional[str] = self.status.error_message.value
            running = False
            skipped = False
        elif isinstance(self.status, job_status.JobInProgress):
            error_occurred = None
            error_message = None
            running = True
            skipped = False
        elif isinstance(self.status, job_status.JobSkipped):
            error_occurred = False
            error_message = None
            running = False
            skipped = True
        elif isinstance(self.status, job_status.JobRanSuccessfully):
            error_occurred = False
            error_message = None
            running = False
            skipped = False
        else:
            raise ValueError(f"Expected an instance of job_status.JobStatus, but got {self.status!r}.")

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
            execution_error_message=error_message,
            running=running,
            skipped=skipped,
            ts=self.ts.value,
        )

    @staticmethod
    def running(
        *,
        job_status_id: value_objects.UniqueId,
        batch_id: value_objects.UniqueId,
        job_name: value_objects.JobName,
        ts: value_objects.Timestamp,
    ) -> JobResult:
        return JobResult(
            id=job_status_id,
            batch_id=batch_id,
            job_name=job_name,
            ts=ts,
            test_results=frozenset(),
            execution_millis=None,
            status=job_status.JobStatus.in_progress(),
        )
