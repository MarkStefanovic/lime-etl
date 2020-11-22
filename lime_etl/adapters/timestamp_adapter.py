from __future__ import annotations

import abc
import datetime
import typing

import lime_uow as lu

from lime_etl import domain


__all__ = (
    "TimestampAdapter",
    "LocalTimestampAdapter",
)


class TimestampAdapter(lu.Resource[None], abc.ABC):
    @abc.abstractmethod
    def now(self) -> domain.Timestamp:
        raise NotImplementedError

    def get_elapsed_time(self, start_ts: domain.Timestamp) -> domain.ExecutionMillis:
        end_ts = self.now()
        millis = int((end_ts.value - start_ts.value).total_seconds() * 1000)
        return domain.ExecutionMillis(millis)


class LocalTimestampAdapter(TimestampAdapter):
    @classmethod
    def interface(cls) -> typing.Type[TimestampAdapter]:
        return TimestampAdapter

    def now(self) -> domain.Timestamp:
        return domain.Timestamp(datetime.datetime.now())
