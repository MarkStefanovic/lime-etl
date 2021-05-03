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
        ts_adapter: domain.TimestampAdapter = domain.LocalTimestampAdapter(),
        log_to_console: bool = False,
    ):
        super().__init__()

        self._batch_id = batch_id
        self._session = session
        self._ts_adapter = ts_adapter
        self._log_to_console = log_to_console

    def create_job_logger(self, /, job_id: domain.UniqueId) -> domain.JobLogger:
        return sqlalchemy_job_logger.SqlAlchemyJobLogger(
            batch_id=self._batch_id,
            job_id=job_id,
            session=self._session,
            ts_adapter=self._ts_adapter,
            log_to_console=self._log_to_console,
        )

    def _log(self, level: domain.LogLevel, message: str) -> None:
        ts = self._ts_adapter.now()
        log_entry = domain.BatchLogEntry(
            id=domain.UniqueId.generate(),
            batch_id=self._batch_id,
            log_level=level,
            message=domain.LogMessage(message),
            ts=ts,
        )
        self._session.add(log_entry.to_dto())
        self._session.commit()
        if self._log_to_console:
            print(f"{ts.value.strftime('%H:%M:%S')} [{level.value!s}]: {message}")

    def error(self, /, message: str) -> None:
        return self._log(
            level=domain.LogLevel.error(),
            message=message,
        )

    def exception(self, /, e: Exception) -> None:
        msg = domain.exceptions.parse_exception(e).text()
        self._log(
            level=domain.LogLevel.error(),
            message=msg,
        )

    def info(self, /, message: str) -> None:
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

    def error(self, /, message: str) -> None:
        print(f"ERROR: {message}")

    def exception(self, /, e: Exception) -> None:
        print(f"EXCEPTION:\n{domain.exceptions.parse_exception(e).text()}")

    def info(self, /, message: str) -> None:
        print(f"INFO: {message}")
