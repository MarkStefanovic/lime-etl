from sqlalchemy import orm

from lime_etl import domain
from lime_etl.adapter import sqlalchemy_job_logger

__all__ = (
    "SqlAlchemyBatchLogger",
    "ConsoleBatchLogger",
)


class SqlAlchemyBatchLogger(domain.BatchLogger):
    def __init__(
        self,
        *,
        batch_id: domain.UniqueId,
        session: orm.Session,
        ts_adapter: domain.TimestampAdapter,
    ):
        self._batch_id = batch_id
        self._session = session
        self._ts_adapter = ts_adapter
        super().__init__()

    def create_job_logger(self, /, job_id: domain.UniqueId) -> domain.JobLogger:
        return sqlalchemy_job_logger.SqlAlchemyJobLogger(
            batch_id=self._batch_id,
            job_id=job_id,
            session=self._session,
            ts_adapter=self._ts_adapter,
        )

    def _log(self, level: domain.LogLevel, message: str) -> None:
        log_entry = domain.BatchLogEntry(
            id=domain.UniqueId.generate(),
            batch_id=self._batch_id,
            log_level=level,
            message=domain.LogMessage(message),
            ts=self._ts_adapter.now(),
        )
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


class ConsoleBatchLogger(domain.BatchLogger):
    def __init__(self, batch_id: domain.UniqueId):
        self.batch_id = batch_id
        super().__init__()

    def create_job_logger(self, /, job_id: domain.UniqueId) -> domain.JobLogger:
        return sqlalchemy_job_logger.ConsoleJobLogger()

    def log_error(self, message: str, /) -> None:
        print(f"ERROR: {message}")
        return None

    def log_info(self, message: str, /) -> None:
        print(f"INFO: {message}")
        return None
