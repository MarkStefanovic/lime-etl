import abc


from src.domain import job_log_entry, value_objects
from src.services import unit_of_work


class JobLoggingService(abc.ABC):
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
    def log_start(
        self, uow: unit_of_work.UnitOfWork,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_completed_successfully(
        self, uow: unit_of_work.UnitOfWork,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_retry(
        self, uow: unit_of_work.UnitOfWork, retry: int, max_retries: int,
    ) -> None:
        raise NotImplementedError


class DefaultJobLoggingService(JobLoggingService):
    def __init__(
        self, batch_id: value_objects.UniqueId, job_id: value_objects.UniqueId
    ):
        self.batch_id = batch_id
        self.job_id = job_id

        super().__init__()

    def _log(
        self,
        uow: unit_of_work.UnitOfWork,
        level: value_objects.LogLevel,
        message: value_objects.LogMessage,
    ) -> None:
        with uow:
            ts = uow.ts_adapter.now()
            log_entry = job_log_entry.JobLogEntry(
                id=value_objects.UniqueId.generate(),
                batch_id=self.batch_id,
                job_id=self.job_id,
                log_level=level,
                message=message,
                ts=ts,
            )
            print(log_entry)
            uow.job_log.add(log_entry=log_entry)
            uow.commit()
            return None

    def log_error(
        self, uow: unit_of_work.UnitOfWork, message: value_objects.LogMessage,
    ) -> None:
        return self._log(
            uow=uow, level=value_objects.LogLevel.error(), message=message,
        )

    def log_info(
        self, uow: unit_of_work.UnitOfWork, message: value_objects.LogMessage,
    ) -> None:
        return self._log(uow=uow, level=value_objects.LogLevel.info(), message=message,)

    def log_start(
        self, uow: unit_of_work.UnitOfWork,
    ) -> None:
        return self._log(
            uow=uow,
            level=value_objects.LogLevel.info(),
            message=value_objects.LogMessage("Job started."),
        )

    def log_completed_successfully(
        self, uow: unit_of_work.UnitOfWork,
    ) -> None:
        return self._log(
            uow=uow,
            level=value_objects.LogLevel.info(),
            message=value_objects.LogMessage("Job completed successfully."),
        )

    def log_retry(
        self, uow: unit_of_work.UnitOfWork, retry: int, max_retries: int,
    ) -> None:
        return self._log(
            uow=uow,
            level=value_objects.LogLevel.info(),
            message=value_objects.LogMessage(
                f"Running retry {retry} of {max_retries}..."
            ),
        )


class ConsoleJobLoggingService(JobLoggingService):
    def __init__(self) -> None:
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
        print("INFO: Job started.")
        return None

    def log_completed_successfully(
        self, uow: unit_of_work.UnitOfWork,
    ) -> None:
        print("INFO: Job completed successfully.")
        return None

    def log_retry(
        self, uow: unit_of_work.UnitOfWork, retry: int, max_retries: int,
    ) -> None:
        print(f"INFO: Retry {retry}.")
        return None
