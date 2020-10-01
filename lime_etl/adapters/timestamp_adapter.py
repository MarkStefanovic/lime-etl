from __future__ import annotations

import abc
import datetime

from lime_etl.domain import value_objects


class TimestampAdapter(abc.ABC):
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
    def now(self) -> value_objects.Timestamp:
        return value_objects.Timestamp(datetime.datetime.now())
