from sqlalchemy import orm

from lime_etl import domain

__all__ = (
    "SqlAlchemyJobLoggingService",
    "ConsoleJobLoggingService",
)


class SqlAlchemyJobLoggingService(domain.JobLoggingService):
    def __init__(
        self,
        batch_id: domain.UniqueId,
        job_id: domain.UniqueId,
        session: orm.Session,
        ts_adapter: domain.TimestampAdapter,
    ):
        self._batch_id = batch_id
        self._job_id = job_id
        self._session = session
        self._ts_adapter = ts_adapter

        super().__init__()

    def _log(self, level: domain.LogLevel, message: str) -> None:
        log_entry = domain.JobLogEntry(
            id=domain.UniqueId.generate(),
            batch_id=self._batch_id,
            job_id=self._job_id,
            log_level=level,
            message=domain.LogMessage(message),
            ts=self._ts_adapter.now(),
        )
        print(log_entry)
        self._session.add(log_entry.to_dto())
        self._session.commit()
        return None

    def log_error(self, message: str, /) -> None:
        return self._log(
            level=domain.LogLevel.error(),
            message=message,
        )

    def log_info(self, message: str, /) -> None:
        return self._log(
            level=domain.LogLevel.info(),
            message=message,
        )


class ConsoleJobLoggingService(domain.JobLoggingService):
    def __init__(self) -> None:
        super().__init__()

    def log_error(self, message: str, /) -> None:
        print(f"ERROR: {message}")
        return None

    def log_info(self, message: str, /) -> None:
        print(f"INFO: {message}")
        return None
