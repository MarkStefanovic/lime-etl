import abc


from lime_etl.domain import batch_log_entry, value_objects
from lime_etl.services import unit_of_work


class BatchLoggingService(abc.ABC):
    @abc.abstractmethod
    def log_error(self, message: str, /) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_info(self, message: str, /) -> None:
        raise NotImplementedError


class DefaultBatchLoggingService(BatchLoggingService):
    def __init__(self, uow: unit_of_work.UnitOfWork, batch_id: value_objects.UniqueId):
        self._uow = uow
        self.batch_id = batch_id
        super().__init__()

    def _log(self, level: value_objects.LogLevel, message: str) -> None:
        with self._uow as uow:
            ts = uow.ts_adapter.now()
            log_entry = batch_log_entry.BatchLogEntry(
                id=value_objects.UniqueId.generate(),
                batch_id=self.batch_id,
                log_level=level,
                message=value_objects.LogMessage(message),
                ts=ts,
            )
            print(log_entry)
            uow.batch_log.add(log_entry)
            uow.commit()
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


class ConsoleBatchLoggingService(BatchLoggingService):
    def __init__(self, batch_id: value_objects.UniqueId):
        self.batch_id = batch_id
        super().__init__()

    def log_error(self, message: str, /) -> None:
        print(f"ERROR: {message}")
        return None

    def log_info(self, message: str, /) -> None:
        print(f"INFO: {message}")
        return None
