import abc
import functools
import typing

import lime_uow as lu

from lime_etl import domain, adapters
from lime_etl.services import job_spec

UoW = typing.TypeVar("UoW", bound=lu.UnitOfWork)

__all__ = ("BatchSpec",)


class BatchSpec(abc.ABC, typing.Generic[UoW]):
    @functools.cached_property
    def batch_id(self) -> domain.UniqueId:
        return domain.UniqueId.generate()

    @property
    @abc.abstractmethod
    def batch_name(self) -> domain.BatchName:
        raise NotImplementedError

    @abc.abstractmethod
    def create_jobs(self, uow: UoW) -> typing.List[job_spec.JobSpec[UoW]]:
        raise NotImplementedError

    @abc.abstractmethod
    def create_uow(self) -> UoW:
        raise NotImplementedError

    @property
    def skip_tests(self) -> domain.Flag:
        return domain.Flag(False)

    @property
    def timeout_seconds(self) -> typing.Optional[domain.TimeoutSeconds]:
        return domain.TimeoutSeconds(None)

    @property
    def ts_adapter(self) -> domain.TimestampAdapter:
        return adapters.LocalTimestampAdapter()

    def __repr__(self) -> str:
        return f"<BatchSpec: {self.__class__.__name__}>: {self.batch_name.value}"

    def __hash__(self) -> int:
        return hash(self.batch_name.value)

    def __eq__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return (
                self.batch_name.value
                == typing.cast(BatchSpec[typing.Any], other).batch_name.value
            )
        else:
            return NotImplemented
