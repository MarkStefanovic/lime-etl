from __future__ import annotations

import dataclasses

from lime_etl.domain import value_objects


__all__ = (
    "JobRanSuccessfully",
    "JobFailed",
    "JobInProgress",
    "JobStatus",
    "JobSkipped",
)


@dataclasses.dataclass(frozen=True)
class JobStatus:
    @staticmethod
    def failed(error_message: str, /) -> JobFailed:
        msg = value_objects.LogMessage(error_message)
        return JobFailed(msg)

    @staticmethod
    def in_progress() -> JobInProgress:
        return JobInProgress()

    @staticmethod
    def skipped(reason: str, /) -> JobSkipped:
        msg = value_objects.LogMessage(reason)
        return JobSkipped(reason=msg)

    @staticmethod
    def success() -> JobRanSuccessfully:
        return JobRanSuccessfully()


@dataclasses.dataclass(frozen=True)
class JobFailed(JobStatus):
    error_message: value_objects.LogMessage


@dataclasses.dataclass(frozen=True)
class JobRanSuccessfully(JobStatus):
    ...


@dataclasses.dataclass(frozen=True)
class JobSkipped(JobStatus):
    reason: value_objects.LogMessage


@dataclasses.dataclass(frozen=True)
class JobInProgress(JobStatus):
    ...
