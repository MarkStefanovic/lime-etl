import abc
import typing

import lime_uow as lu

from lime_etl.adapters import timestamp_adapter
from lime_etl.domain import job_spec, value_objects

UOW = typing.TypeVar("UOW", bound=lu.UnitOfWork)


class BatchSpec(abc.ABC, typing.Generic[UOW]):
    def __init__(
        self,
        batch_name: value_objects.BatchName,
        batch_id: typing.Optional[value_objects.UniqueId] = None,
        skip_tests: value_objects.Flag = value_objects.Flag(False),
        timeout_seconds: typing.Optional[value_objects.TimeoutSeconds] = None,
        ts_adapter: timestamp_adapter.TimestampAdapter = timestamp_adapter.LocalTimestampAdapter(),
    ):
        self._batch_name = batch_name
        self._batch_id = batch_id
        self._skip_tests = skip_tests
        self._timeout_seconds = timeout_seconds
        self._ts_adapter = ts_adapter

        self._job_specs: typing.Optional[typing.Tuple[job_spec.JobSpec, ...]] = None
        self._shared_resources: typing.Optional[lu.SharedResources] = None
        self._uow: typing.Optional[UOW] = None

    @property
    def batch_id(self) -> value_objects.UniqueId:
        if self._batch_id is None:
            self._batch_id = value_objects.UniqueId.generate()
        return self._batch_id

    @property
    def batch_name(self) -> value_objects.BatchName:
        return self._batch_name

    @abc.abstractmethod
    def create_job_specs(self, uow: UOW) -> typing.Collection[job_spec.JobSpec]:
        raise NotImplementedError

    @abc.abstractmethod
    def create_shared_resource(self) -> lu.SharedResources:
        raise NotImplementedError

    @abc.abstractmethod
    def create_uow(self, shared_resources: lu.SharedResources) -> UOW:
        raise NotImplementedError

    @property
    def job_specs(self) -> typing.Tuple[job_spec.JobSpec, ...]:
        if self._job_specs is None:
            self._job_specs = tuple(self.create_job_specs(self.uow))
        return self._job_specs

    @property
    def shared_resources(self) -> lu.SharedResources:
        if self._shared_resources is None:
            self._shared_resources = self.create_shared_resource()
        return self._shared_resources

    @property
    def skip_tests(self) -> value_objects.Flag:
        return self._skip_tests

    @property
    def timeout_seconds(self) -> typing.Optional[value_objects.TimeoutSeconds]:
        return self._timeout_seconds

    @property
    def ts_adapter(self) -> timestamp_adapter.TimestampAdapter:
        return self._ts_adapter

    @property
    def uow(self) -> UOW:
        if self._uow is None:
            self._uow = self.create_uow(self.shared_resources)
        return self._uow

    def __repr__(self) -> str:
        return f"<BatchSpec: {self.__class__.__name__}>: {self.batch_name.value}"

    def __hash__(self) -> int:
        return hash(self.batch_name.value)

    def __eq__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return (
                self.batch_name.value == typing.cast(BatchSpec[typing.Any], other).batch_name.value
            )
        else:
            return NotImplemented
