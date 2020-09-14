from __future__ import annotations

import abc
import datetime

from domain import value_objects


class TimestampAdapter(abc.ABC):
    @abc.abstractmethod
    def now(self) -> value_objects.Timestamp:
        raise NotImplementedError


class LocalTimestampAdapter(TimestampAdapter):
    def now(self) -> value_objects.Timestamp:
        return value_objects.Timestamp(datetime.datetime.now())
