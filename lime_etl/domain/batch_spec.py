import abc
import functools
import typing

import lime_uow as lu

from lime_etl.domain import cfg, job_spec, value_objects

__all__ = (
    "BatchSpec",
    "create_batch",
)

Cfg = typing.TypeVar("Cfg", bound=cfg.Config)
UoW = typing.TypeVar("UoW", bound=lu.UnitOfWork)


class BatchSpec(abc.ABC, typing.Generic[Cfg, UoW]):
    @functools.cached_property
    def batch_id(self) -> value_objects.UniqueId:
        return value_objects.UniqueId.generate()

    @property
    @abc.abstractmethod
    def batch_name(self) -> value_objects.BatchName:
        raise NotImplementedError

    @abc.abstractmethod
    def create_jobs(self, uow: UoW) -> typing.List[job_spec.JobSpec[UoW]]:
        raise NotImplementedError

    @abc.abstractmethod
    def create_uow(self, config: Cfg) -> UoW:
        raise NotImplementedError

    @property
    def skip_tests(self) -> value_objects.Flag:
        return value_objects.Flag(False)

    @property
    def timeout_seconds(self) -> value_objects.TimeoutSeconds:
        return value_objects.TimeoutSeconds(None)

    def __repr__(self) -> str:
        return f"<BatchSpec: {self.__class__.__name__}>: {self.batch_name.value}"

    def __hash__(self) -> int:
        return hash(self.batch_name.value)

    def __eq__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return (
                self.batch_name.value
                == typing.cast(
                    BatchSpec[typing.Any, typing.Any], other
                ).batch_name.value
            )
        else:
            return NotImplemented


class BatchSpecImpl(BatchSpec[Cfg, UoW]):
    def __init__(
        # fmt: off
        self,
        *,
        name: str,
        create_uow: typing.Callable[[Cfg], UoW],
        jobs: typing.List[job_spec.JobSpec[UoW]],
        runtime_jobs: typing.Optional[
            typing.Callable[[UoW], typing.List[job_spec.JobSpec[UoW]]]
        ],
        skip_tests: bool,
        timeout_seconds: typing.Optional[int],
        # fmt: on
    ):
        self._batch_name = value_objects.BatchName(name)
        self._jobs = jobs
        self._runtime_jobs = runtime_jobs
        self._create_uow = create_uow
        self._skip_tests = value_objects.Flag(skip_tests)
        self._timeout_seconds = value_objects.TimeoutSeconds(timeout_seconds)

    @functools.cached_property
    def batch_id(self) -> value_objects.UniqueId:
        return value_objects.UniqueId.generate()

    @property
    def batch_name(self) -> value_objects.BatchName:
        return self._batch_name

    def create_jobs(self, uow: UoW) -> typing.List[job_spec.JobSpec[UoW]]:
        if self._runtime_jobs:
            return self._runtime_jobs(uow) + self._jobs
        else:
            return self._jobs

    def create_uow(self, config: Cfg) -> UoW:
        return self._create_uow(config)

    @property
    def skip_tests(self) -> value_objects.Flag:
        return value_objects.Flag(False)

    @property
    def timeout_seconds(self) -> value_objects.TimeoutSeconds:
        return value_objects.TimeoutSeconds(None)


def create_batch(
    # fmt: off
    *,
    name: str,
    jobs: typing.List[job_spec.JobSpec[UoW]],
    create_uow: typing.Callable[[Cfg], UoW],
    runtime_jobs: typing.Optional[
        typing.Callable[
            [UoW],
            typing.List[job_spec.JobSpec[UoW]]
        ]
    ] = None,
    skip_tests: bool = False,
    timeout_seconds: typing.Optional[int] = None,
    # fmt: on
) -> BatchSpec[Cfg, UoW]:
    return typing.cast(
        BatchSpec[Cfg, UoW],
        BatchSpecImpl(
            name=name,
            jobs=jobs,
            runtime_jobs=runtime_jobs,
            create_uow=create_uow,
            skip_tests=skip_tests,
            timeout_seconds=timeout_seconds,
        ),
    )
