import abc


from src.domain import batch_log_entry, value_objects
from src.services import unit_of_work


class BatchLoggingService(abc.ABC):
    @abc.abstractmethod
    def log_error(
        self, uow: unit_of_work.UnitOfWork, message: value_objects.LogMessage,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_info(
        self, uow: unit_of_work.UnitOfWork, message: value_objects.LogMessage,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_start(self, uow: unit_of_work.UnitOfWork,) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_completed_successfully(self, uow: unit_of_work.UnitOfWork,) -> None:
        raise NotImplementedError


class DefaultBatchLoggingService(BatchLoggingService):
    def __init__(self, batch_id: value_objects.UniqueId):
        self.batch_id = batch_id
        super().__init__()

    def _log(
        self,
        uow: unit_of_work.UnitOfWork,
        batch_id: value_objects.UniqueId,
        level: value_objects.LogLevel,
        message: value_objects.LogMessage,
    ) -> None:
        with uow:
            ts = uow.ts_adapter.now()
            log_entry = batch_log_entry.BatchLogEntry(
                id=value_objects.UniqueId.generate(),
                batch_id=batch_id,
                log_level=level,
                message=message,
                ts=ts,
            )
            print(log_entry)
            uow.batch_log.add(log_entry)
            uow.commit()
            return None

    def log_error(
        self, uow: unit_of_work.UnitOfWork, message: value_objects.LogMessage,
    ) -> None:
        with uow:
            return self._log(
                uow=uow,
                batch_id=self.batch_id,
                level=value_objects.LogLevel.error(),
                message=message,
            )

    def log_info(
        self, uow: unit_of_work.UnitOfWork, message: value_objects.LogMessage,
    ) -> None:
        with uow:
            return self._log(
                uow=uow,
                batch_id=self.batch_id,
                level=value_objects.LogLevel.info(),
                message=message,
            )

    def log_start(self, uow: unit_of_work.UnitOfWork) -> None:
        with uow:
            return self._log(
                uow=uow,
                batch_id=self.batch_id,
                level=value_objects.LogLevel.info(),
                message=value_objects.LogMessage("Batch started."),
            )

    def log_completed_successfully(self, uow: unit_of_work.UnitOfWork) -> None:
        with uow:
            return self._log(
                uow=uow,
                batch_id=self.batch_id,
                level=value_objects.LogLevel.info(),
                message=value_objects.LogMessage("Batch completed successfully."),
            )


class ConsoleBatchLoggingService(BatchLoggingService):
    def __init__(self, batch_id: value_objects.UniqueId):
        self.batch_id = batch_id
        super().__init__()

    def log_error(
        self, uow: unit_of_work.UnitOfWork, message: value_objects.LogMessage,
    ) -> None:
        print(f"ERROR: {message.value}")
        return None

    def log_info(
        self, uow: unit_of_work.UnitOfWork, message: value_objects.LogMessage,
    ) -> None:
        print(f"INFO: {message.value}")
        return None

    def log_start(
        self, uow: unit_of_work.UnitOfWork,
    ) -> None:
        print("INFO: Batch started.")
        return None

    def log_completed_successfully(
        self, uow: unit_of_work.UnitOfWork,
    ) -> None:
        print("INFO: Batch completed successfully.")
        return None
