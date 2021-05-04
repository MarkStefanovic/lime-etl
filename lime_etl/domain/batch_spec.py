import abc
import functools
import typing

import lime_uow as lu

from lime_etl.domain import (
    batch_status,
    cfg,
    job_spec,
    timestamp_adapter,
    value_objects,
)

__all__ = (
    "BatchSpec",
    "create_batch",
)

Cfg = typing.TypeVar("Cfg", bound=cfg.Config)


class BatchSpec(abc.ABC, typing.Generic[Cfg]):
    @functools.cached_property
    def batch_id(self) -> value_objects.UniqueId:
        return value_objects.UniqueId.generate()

    @property
    @abc.abstractmethod
    def batch_name(self) -> value_objects.BatchName:
        raise NotImplementedError

    @abc.abstractmethod
    def create_jobs(self, uow: lu.UnitOfWork) -> typing.List[job_spec.JobSpec]:
        raise NotImplementedError

    @abc.abstractmethod
    def create_uow(self, config: Cfg) -> lu.UnitOfWork:
        raise NotImplementedError

    def run(
        self,
        *,
        config: Cfg,
        log_to_console: bool = False,
        ts_adapter: timestamp_adapter.TimestampAdapter = timestamp_adapter.LocalTimestampAdapter(),
    ) -> batch_status.BatchStatus:
        from lime_etl.service import batch_runner

        return batch_runner.run_batch(
            batch=self,
            config=config,
            log_to_console=log_to_console,
            ts_adapter=ts_adapter,
        )

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
            # fmt: off
            return self.batch_name.value == typing.cast(BatchSpec[Cfg], other).batch_name.value
            # fmt: on
        else:
            return NotImplemented


class BatchSpecImpl(BatchSpec[Cfg]):
    def __init__(
        # fmt: off
        self,
        *,
        name: str,
        create_uow: typing.Callable[[Cfg], lu.UnitOfWork],
        jobs: typing.List[job_spec.JobSpec],
        skip_tests: bool,
        timeout_seconds: typing.Optional[int],
        # fmt: on
    ):
        self._batch_name = value_objects.BatchName(name)
        self._jobs = jobs
        self._create_uow = create_uow
        self._skip_tests = value_objects.Flag(skip_tests)
        self._timeout_seconds = value_objects.TimeoutSeconds(timeout_seconds)

    @functools.cached_property
    def batch_id(self) -> value_objects.UniqueId:
        return value_objects.UniqueId.generate()

    @property
    def batch_name(self) -> value_objects.BatchName:
        return self._batch_name

    def create_jobs(self, uow: lu.UnitOfWork) -> typing.List[job_spec.JobSpec]:
        return self._jobs

    def create_uow(self, config: Cfg) -> lu.UnitOfWork:
        return self._create_uow(config)

    @property
    def skip_tests(self) -> value_objects.Flag:
        return self._skip_tests

    @property
    def timeout_seconds(self) -> value_objects.TimeoutSeconds:
        return self._timeout_seconds


def create_batch(
    # fmt: off
    *,
    name: str,
    jobs: typing.List[job_spec.JobSpec],
    create_uow: typing.Callable[[Cfg], lu.UnitOfWork],
    skip_tests: bool = False,
    timeout_seconds: typing.Optional[int] = None,
    # fmt: on
) -> BatchSpec[cfg.Config]:
    return BatchSpecImpl(
        name=name,
        jobs=jobs,
        create_uow=create_uow,  # type: ignore
        skip_tests=skip_tests,
        timeout_seconds=timeout_seconds,
    )
