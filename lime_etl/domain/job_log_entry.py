from __future__ import annotations

import datetime
from dataclasses import dataclass

from lime_etl.domain import value_objects


@dataclass(unsafe_hash=True)
class JobLogEntryDTO:
    id: str
    batch_id: str
    job_id: str
    log_level: value_objects.LogLevelOption
    message: str
    ts: datetime.datetime

    def to_domain(self) -> JobLogEntry:
        return JobLogEntry(
            id=value_objects.UniqueId(self.id),
            batch_id=value_objects.UniqueId(self.batch_id),
            job_id=value_objects.UniqueId(self.job_id),
            log_level=value_objects.LogLevel(self.log_level),
            message=value_objects.LogMessage(self.message),
            ts=value_objects.Timestamp(self.ts),
        )


@dataclass(frozen=True)
class JobLogEntry:
    id: value_objects.UniqueId
    batch_id: value_objects.UniqueId
    job_id: value_objects.UniqueId
    log_level: value_objects.LogLevel
    message: value_objects.LogMessage
    ts: value_objects.Timestamp

    def to_dto(self) -> JobLogEntryDTO:
        return JobLogEntryDTO(
            id=self.id.value,
            batch_id=self.batch_id.value,
            job_id=self.job_id.value,
            log_level=self.log_level.value,
            message=self.message.value,
            ts=self.ts.value,
        )

    def __str__(self) -> str:
        ts_str = self.ts.value.strftime('%H:%M:%S')
        return f"{ts_str} - {self.log_level} - {self.message.value}"
