from __future__ import annotations

import abc
import datetime
import typing

import lime_uow as lu

from lime_etl.domain import value_objects


__all__ = (
    "TimestampAdapter",
    "LocalTimestampAdapter",
)


class TimestampAdapter(lu.Resource[None], abc.ABC):
    @abc.abstractmethod
    def now(self) -> value_objects.Timestamp:
        raise NotImplementedError

    def get_elapsed_time(
        self, start_ts: value_objects.Timestamp
    ) -> value_objects.ExecutionMillis:
        end_ts = self.now()
        millis = int((end_ts.value - start_ts.value).total_seconds() * 1000)
        return value_objects.ExecutionMillis(millis)


class LocalTimestampAdapter(TimestampAdapter):
    def close(self) -> None:
        pass

    @classmethod
    def interface(cls) -> typing.Type[TimestampAdapter]:
        return TimestampAdapter

    def open(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def save(self) -> None:
        pass

    def now(self) -> value_objects.Timestamp:
        return value_objects.Timestamp(datetime.datetime.now())
