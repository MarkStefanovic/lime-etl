from __future__ import annotations

import datetime
import typing

from lime_etl import domain

__all__ = ("LocalTimestampAdapter",)


class LocalTimestampAdapter(domain.TimestampAdapter):
    @classmethod
    def interface(cls) -> typing.Type[domain.TimestampAdapter]:
        return domain.TimestampAdapter

    def now(self) -> domain.Timestamp:
        return domain.Timestamp(datetime.datetime.now())
