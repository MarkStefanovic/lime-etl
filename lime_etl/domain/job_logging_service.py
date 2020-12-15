import abc

__all__ = ("JobLoggingService",)


class JobLoggingService(abc.ABC):
    @abc.abstractmethod
    def log_error(self, message: str, /) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_info(self, message: str, /) -> None:
        raise NotImplementedError
