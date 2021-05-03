from sqlalchemy import orm

from lime_etl import domain

__all__ = (
    "SqlAlchemyJobLogger",
    "ConsoleJobLogger",
)


class SqlAlchemyJobLogger(domain.JobLogger):
    def __init__(
        self,
        *,
        batch_id: domain.UniqueId,
        job_id: domain.UniqueId,
        session: orm.Session,
        ts_adapter: domain.TimestampAdapter = domain.LocalTimestampAdapter(),
        log_to_console: bool = False,
    ):
        self._batch_id = batch_id
        self._job_id = job_id
        self._session = session
        self._ts_adapter = ts_adapter
        self._log_to_console = log_to_console

        super().__init__()

    def _log(self, level: domain.LogLevel, message: str) -> None:
        ts = self._ts_adapter.now()
        log_entry = domain.JobLogEntry(
            id=domain.UniqueId.generate(),
            batch_id=self._batch_id,
            job_id=self._job_id,
            log_level=level,
            message=domain.LogMessage(message),
            ts=ts,
        )
        self._session.add(log_entry.to_dto())
        self._session.commit()
        if self._log_to_console:
            print(f"{ts.value.strftime('%H:%M:%S')} [{level.value!s}]: {message}")

    def error(self, message: str, /) -> None:
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

    def info(self, message: str, /) -> None:
        return self._log(
            level=domain.LogLevel.info(),
            message=message,
        )


class ConsoleJobLogger(domain.JobLogger):
    def __init__(self) -> None:
        super().__init__()

    def error(self, message: str, /) -> None:
        print(f"ERROR: {message}")

    def exception(self, /, e: Exception) -> None:
        print(f"EXCEPTION:\n{domain.exceptions.parse_exception(e).text()}")

    def info(self, message: str, /) -> None:
        print(f"INFO: {message}")
