import abc
import warnings

from sqlalchemy import orm

from lime_etl.adapters import timestamp_adapter
from lime_etl.domain import batch_log_entry, value_objects
from lime_etl.services import job_logging_service

__all__ = (
    "AbstractBatchLoggingService",
    "BatchLoggingService",
    "ConsoleBatchLoggingService",
)


class AbstractBatchLoggingService(abc.ABC):
    @abc.abstractmethod
    def create_job_logger(self) -> job_logging_service.AbstractJobLoggingService:
        raise NotImplementedError

    @abc.abstractmethod
    def log_error(self, message: str, /) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_info(self, message: str, /) -> None:
        raise NotImplementedError


class BatchLoggingService(AbstractBatchLoggingService):
    def __init__(
        self,
        batch_id: value_objects.UniqueId,
        session: orm.Session,
        ts_adapter: timestamp_adapter.TimestampAdapter,
    ):
        self._batch_id = batch_id
        self._session = session
        self._ts_adapter = ts_adapter
        super().__init__()

    def create_job_logger(self) -> job_logging_service.JobLoggingService:
        return job_logging_service.JobLoggingService(
            batch_id=self._batch_id,
            job_id=value_objects.UniqueId.generate(),
            session=self._session,
            ts_adapter=self._ts_adapter,
        )

    def _log(self, level: value_objects.LogLevel, message: str) -> None:
        log_entry = batch_log_entry.BatchLogEntry(
            id=value_objects.UniqueId.generate(),
            batch_id=self._batch_id,
            log_level=level,
            message=value_objects.LogMessage(message),
            ts=self._ts_adapter.now(),
        )
        self._session.add(log_entry.to_dto())
        self._session.commit()
        return None

    def log_error(self, message: str, /) -> None:
        return self._log(
            level=value_objects.LogLevel.error(),
            message=message,
        )

    def log_info(self, message: str, /) -> None:
        return self._log(
            level=value_objects.LogLevel.info(),
            message=message,
        )


class ConsoleBatchLoggingService(AbstractBatchLoggingService):
    def __init__(self, batch_id: value_objects.UniqueId):
        self.batch_id = batch_id
        super().__init__()

    def create_job_logger(self) -> job_logging_service.AbstractJobLoggingService:
        return job_logging_service.ConsoleJobLoggingService()

    def log_error(self, message: str, /) -> None:
        print(f"ERROR: {message}")
        return None

    def log_info(self, message: str, /) -> None:
        print(f"INFO: {message}")
        return None
