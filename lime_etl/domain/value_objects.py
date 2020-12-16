from __future__ import annotations

import datetime
import enum
import warnings
import typing
from uuid import uuid4


__all__ = (
    "ValueObject",
    "BatchName",
    "Days",
    "DaysToKeep",
    "DbUri",
    "ExecutionMillis",
    "Failure",
    "Flag",
    "JobName",
    "LogLevel",
    "LogLevelOption",
    "LogMessage",
    "MaxProcesses",
    "MaxRetries",
    "MinSecondsBetweenRefreshes",
    "NonEmptyStr",
    "Password",
    "PositiveInt",
    "ResourceName",
    "Result",
    "SchemaName",
    "SecondsSinceLastRefresh",
    "SingleChar",
    "Success",
    "TestName",
    "TimeoutSeconds",
    "Timestamp",
    "UniqueId",
)


class ValueObject:
    __slots__ = ("value",)

    def __init__(self, value: typing.Any, /):
        self.value = value

    def __eq__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return self.value == typing.cast(ValueObject, other).value
        else:
            return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return NotImplemented
        else:
            return not result

    def __lt__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return self.value < typing.cast(ValueObject, other).value
        else:
            return NotImplemented

    def __le__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return self.value <= typing.cast(ValueObject, other).value
        else:
            return NotImplemented

    def __gt__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return self.value > typing.cast(ValueObject, other).value
        else:
            return NotImplemented

    def __ge__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return self.value >= typing.cast(ValueObject, other).value
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(self.value)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value!r})"

    def __str__(self) -> str:
        return str(self.value)


class Flag(ValueObject):
    def __init__(self, value: bool, /):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )

        if not isinstance(value, bool):
            raise TypeError(
                f"{self.__class__.__name__} expects an integer, but got {value!r}"
            )

        super().__init__(value)


class OptionalPositiveInt(ValueObject):
    def __init__(self, value: typing.Optional[int], /):
        if value is not None:
            if isinstance(value, int):
                if value < 0:
                    raise ValueError(
                        f"{self.__class__.__name__} value must be positive, but got {value!r}."
                    )
            else:
                raise TypeError(
                    f"{self.__class__.__name__} expects an integer, but got {value!r}"
                )

        super().__init__(value)


class PositiveInt(ValueObject):
    def __init__(self, value: int, /):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )
        elif isinstance(value, int):
            if value < 0:
                raise ValueError(
                    f"{self.__class__.__name__} value must be positive, but got {value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects an integer, but got {value!r}"
            )

        super().__init__(value)


class NonEmptyStr(ValueObject):
    def __init__(self, value: str, /):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )
        elif isinstance(value, str):
            if not value:
                raise ValueError(
                    f"{self.__class__.__name__} value is required, but got {value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects a string, but got {value!r}."
            )

        super().__init__(value)


class Success(ValueObject):
    def __init__(self) -> None:
        super().__init__(None)

    def __repr__(self) -> str:
        return "Success()"

    def __str__(self) -> str:
        return "Success"


class Failure(ValueObject):
    def __init__(self, message: str, /) -> None:
        value = NonEmptyStr(message).value
        super().__init__(value)


class Result(ValueObject):
    def __init__(self, value: typing.Union[Failure, Success], /) -> None:
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )

        if not isinstance(value, (Failure, Success)):
            raise TypeError(
                f"{self.__class__.__name__} expects either a Success or Failure instance, "
                f"but got {value!r}."
            )

        super().__init__(value)

    @classmethod
    def failure(cls, message: str, /) -> Result:
        return Result(Failure(message))

    @classmethod
    def success(cls) -> Result:
        return Result(Success())

    @property
    def failure_message(self) -> str:
        if self.is_failure:
            return self.value.value
        else:
            raise TypeError(
                f"{self.__class__.__name__} does not contain a failure value, so it does not have "
                f"a failure message.  The value of the Result is {self.value!r}."
            )

    @property
    def failure_message_or_none(self) -> typing.Optional[str]:
        if self.is_failure:
            return self.value.value
        else:
            return None

    @property
    def is_failure(self) -> bool:
        return isinstance(self.value, Failure)

    @property
    def is_success(self) -> bool:
        return not self.is_failure


class UniqueId(ValueObject):
    def __init__(self, value: str, /):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )
        elif isinstance(value, str):
            if len(value) != 32:
                raise ValueError(
                    f"{self.__class__.__name__} value must be 32 characters long, "
                    f"but got {value!r}."
                )
            if not value.isalnum():
                raise ValueError(
                    f"{self.__class__.__name__} value must be all alphanumeric characters, but "
                    f"got {value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects a str, but got {value!r}"
            )

        super().__init__(value)

    @classmethod
    def generate(cls) -> UniqueId:
        return UniqueId(uuid4().hex)


class SchemaName(ValueObject):
    def __init__(self, value: typing.Optional[str], /):
        if value is None:
            ...
        elif isinstance(value, str):
            if not value:
                raise ValueError(
                    f"If a {self.__class__.__name__} value is provided, then it must be at least 1 "
                    f"character long, but got {value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects a str, but got {value!r}"
            )

        super().__init__(value)


class SingleChar(ValueObject):
    def __init__(self, value: str, /):
        if not value:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got {value!r}."
            )
        elif isinstance(value, str):
            if len(value) != 1:
                raise ValueError(
                    f"{self.__class__.__name__} must be 1 char long, but got {value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects a str, but got {value!r}"
            )

        super().__init__(value)


class _DbName(ValueObject):
    def __init__(self, value: str, /):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )
        elif isinstance(value, str):
            if len(value) < 3 or len(value) >= 200:
                raise ValueError(
                    f"{self.__class__.__name__} must be between 3 and 200 characters long, but got "
                    f"{value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects a str, but got {value!r}"
            )

        super().__init__(value)


class DbUri(NonEmptyStr):
    def __init__(self, value: str, /):
        super().__init__(value)


class BatchName(_DbName):
    def __init__(self, value: str, /):
        super().__init__(value)


class JobName(_DbName):
    def __init__(self, value: str, /):
        super().__init__(value)


class ResourceName(NonEmptyStr):
    ...


class MinSecondsBetweenRefreshes(ValueObject):
    def __init__(self, value: typing.Optional[int], /):
        if value is None:
            ...
        elif isinstance(value, int):
            if value < 0:
                raise ValueError(
                    f"If a {self.__class__.__name__} is provided, then it must be positive, but "
                    f"got {value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects an int, but got {value!r}"
            )

        super().__init__(value)


class TestName(ValueObject):
    def __init__(self, value: str, /):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )
        elif isinstance(value, str):
            if len(value) < 3 or len(value) > 200:
                raise ValueError(
                    f"{self.__class__.__name__} must be between 3 and 200 characters long, "
                    f"but got {value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects a str, but got {value!r}"
            )

        super().__init__(value)


class Days(PositiveInt):
    ...


class DaysToKeep(PositiveInt):
    ...


class ExecutionMillis(PositiveInt):
    ...

    @staticmethod
    def calculate(*, start_time: Timestamp, end_time: Timestamp) -> ExecutionMillis:
        elapsed_millis = int((end_time.value - start_time.value).total_seconds() * 1000)
        return ExecutionMillis(elapsed_millis)


class TimeoutSeconds(ValueObject):
    def __init__(self, value: typing.Optional[int], /):
        if value is None:
            ...
        elif isinstance(value, int):
            if value < 0:
                raise ValueError(
                    f"If a value is provided for {self.__class__.__name__}, then it must be positive."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects an int, but got {value!r}"
            )

        super().__init__(value)


class MaxProcesses(OptionalPositiveInt):
    ...


class MaxRetries(PositiveInt):
    ...


class Timestamp(ValueObject):
    def __init__(self, value: datetime.datetime, /):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )

        if not isinstance(value, datetime.datetime):
            raise TypeError(
                f"{self.__class__.__name__} expects a datetime.datetime, but got {value!r}"
            )

        super().__init__(value)

    @classmethod
    def now(cls) -> Timestamp:
        return Timestamp(datetime.datetime.now())


class Password(ValueObject):
    def __init__(self, value: str, /):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )
        elif isinstance(value, str):
            if not value.strip():
                raise ValueError(
                    f"{self.__class__.__name__} value is required, but got {value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects a str, but got {value!r}"
            )

        super().__init__(value)

    def __repr__(self) -> str:
        return f"Password({'*' * 10})"

    def __str__(self) -> str:
        return "*" * 10


class LogLevelOption(enum.Enum):
    Debug = 1
    Info = 2
    Error = 3


class LogLevel(ValueObject):
    def __init__(self, value: LogLevelOption, /):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )

        if not isinstance(value, LogLevelOption):
            raise TypeError(
                f"{self.__class__.__name__} expects a LogLevelOption value, but got {value!r}."
            )

        super().__init__(value)

    @classmethod
    def debug(cls) -> LogLevel:
        return LogLevel(LogLevelOption.Debug)

    @classmethod
    def error(cls) -> LogLevel:
        return LogLevel(LogLevelOption.Error)

    @classmethod
    def info(cls) -> LogLevel:
        return LogLevel(LogLevelOption.Info)

    def __str__(self) -> str:
        if self.value == LogLevelOption.Debug:
            return "DEBUG"
        elif self.value == LogLevelOption.Info:
            return "INFO"
        elif self.value == LogLevelOption.Error:
            return "ERROR"
        else:
            raise ValueError(f"{self.value} is not a LogLevelOption value.")


class LogMessage(ValueObject):
    def __init__(self, value: str, /):
        if not value:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got {value!r}."
            )

        value = str(value)
        if len(value) > 2000:
            warnings.warn(
                f"{self.__class__.__name__} must be <= 2000 characters long, but the message "
                f"is {len(value)}. It has been truncated to fit."
            )
            value = value[-2000:]

        super().__init__(value)


class SecondsSinceLastRefresh(ValueObject):
    def __init__(self, value: typing.Optional[int], /):
        if value is None:
            ...
        elif isinstance(value, int):
            if not value:
                raise ValueError(
                    f"If a {self.__class__.__name__} value is provided, then it must be positive, "
                    f"but got {value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects an int, but got {value!r}"
            )

        super().__init__(value)
