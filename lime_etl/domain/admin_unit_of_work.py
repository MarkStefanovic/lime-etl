import abc

import lime_uow as lu

from lime_etl.domain import (
    batch_repository,
    batch_log_repository,
    job_repository,
    job_log_repository,
    timestamp_adapter,
)

__all__ = ("AdminUnitOfWork",)


class AdminUnitOfWork(lu.UnitOfWork, abc.ABC):
    @property
    @abc.abstractmethod
    def batch_repo(self) -> batch_repository.BatchRepository:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def batch_log_repo(self) -> batch_log_repository.BatchLogRepository:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def job_repo(self) -> job_repository.JobRepository:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def job_log_repo(self) -> job_log_repository.JobLogRepository:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def ts_adapter(self) -> timestamp_adapter.TimestampAdapter:
        raise NotImplementedError
