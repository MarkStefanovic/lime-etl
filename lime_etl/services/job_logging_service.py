import abc

from sqlalchemy import orm

from lime_etl.adapters import timestamp_adapter
from lime_etl.domain import job_log_entry, value_objects


class AbstractJobLoggingService(abc.ABC):
    @abc.abstractmethod
    def log_error(self, message: str, /) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_info(self, message: str, /) -> None:
        raise NotImplementedError


class JobLoggingService(AbstractJobLoggingService):
    def __init__(
        self,
        batch_id: value_objects.UniqueId,
        job_id: value_objects.UniqueId,
        session: orm.Session,
        ts_adapter: timestamp_adapter.TimestampAdapter,
    ):
        self._batch_id = batch_id
        self._job_id = job_id
        self._session = session
        self._ts_adapter = ts_adapter

        super().__init__()

    def _log(self, level: value_objects.LogLevel, message: str) -> None:
        log_entry = job_log_entry.JobLogEntry(
            id=value_objects.UniqueId.generate(),
            batch_id=self._batch_id,
            job_id=self._job_id,
            log_level=level,
            message=value_objects.LogMessage(message),
            ts=self._ts_adapter.now(),
        )
        print(log_entry)
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


class ConsoleJobLoggingService(AbstractJobLoggingService):
    def __init__(self) -> None:
        super().__init__()

    def log_error(self, message: str, /) -> None:
        print(f"ERROR: {message}")
        return None

    def log_info(self, message: str, /) -> None:
        print(f"INFO: {message}")
        return None
