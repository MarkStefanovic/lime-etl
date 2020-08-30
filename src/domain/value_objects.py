from __future__ import annotations

import datetime
import enum
import re
from typing import Any, Optional, Union, cast
from uuid import uuid4


class ValueObject:
    __slots__ = ("value",)

    def __init__(self, value: Any):
        self.value = value

    def __eq__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return self.value == cast(ValueObject, other).value
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
            return self.value < cast(ValueObject, other).value
        else:
            return NotImplemented

    def __le__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return self.value <= cast(ValueObject, other).value
        else:
            return NotImplemented

    def __gt__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return self.value > cast(ValueObject, other).value
        else:
            return NotImplemented

    def __ge__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return self.value >= cast(ValueObject, other).value
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(self.value)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value!r})"


class Flag(ValueObject):
    def __init__(self, value: bool):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )

        if not isinstance(value, bool):
            raise TypeError(
                f"{self.__class__.__name__} expects an integer, but got {value!r}"
            )

        super().__init__(value=value)


class _PositiveInt(ValueObject):
    def __init__(self, value: int):
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

        super().__init__(value=value)


class _NonEmptyStr(ValueObject):
    def __init__(self, value: str):
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

        super().__init__(value=value)


class Success(ValueObject):
    def __init__(self) -> None:
        super().__init__(value=None)

    def __repr__(self) -> str:
        return "Success()"

    def __str__(self) -> str:
        return "Success"


class Failure(ValueObject):
    def __init__(self, message: str) -> None:
        value = _NonEmptyStr(value=message).value
        super().__init__(value=value)


class Result(ValueObject):
    def __init__(self, value: Union[Failure, Success]) -> None:
        if value is None:
            raise ValueError(f"{self.__class__.__name__} value is required, but got None.")

        if not isinstance(value, (Failure, Success)):
            raise TypeError(
                f"{self.__class__.__name__} expects either a Success or Failure instance, "
                f"but got {value!r}."
            )

        super().__init__(value=value)

    @classmethod
    def failure(cls, message: str) -> Result:
        return Result(value=Failure(message=message))

    @classmethod
    def success(cls) -> Result:
        return Result(value=Success())

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
    def failure_message_or_none(self) -> Optional[str]:
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
    def __init__(self, value: str):
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

        super().__init__(value=value)

    @classmethod
    def generate(cls) -> UniqueId:
        return UniqueId(value=uuid4().hex)


class SchemaName(ValueObject):
    def __init__(self, value: Optional[str]):
        if value is None:
            ...
        elif isinstance(value, str):
            if not value:
                raise ValueError(
                    f"If a {self.__class__.__name__} value is provided, then it must be at least 1 "
                    f"character long, but got {value!r}."
                )
        else:
            raise TypeError(f"{self.__class__.__name__} expects a str, but got {value!r}")

        super().__init__(value=value)


class SingleChar(ValueObject):
    def __init__(self, value: str):
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

        super().__init__(value=value)


class JobName(ValueObject):
    def __init__(self, value: str):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )
        elif isinstance(value, str):
            if len(value) < 3 or len(value) >= 100:
                raise ValueError(
                    f"{self.__class__.__name__} must be between 3 and 100 characters long, but got "
                    f"{value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects a str, but got {value!r}"
            )

        super().__init__(value=value)


class SecondsBetweenRefreshes(ValueObject):
    def __init__(self, value: int):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )
        elif isinstance(value, int):
            if value < 300:
                raise ValueError(
                    f"If a {self.__class__.__name__} must be at least 300 seconds, but got {value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects an int, but got {value!r}"
            )

        super().__init__(value=value)


class TestName(ValueObject):
    def __init__(self, value: str):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )
        elif isinstance(value, str):
            if len(value) < 3 or len(value) > 100:
                raise ValueError(
                    f"{self.__class__.__name__} must be between 3 and 100 characters long, "
                    f"but got {value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects a str, but got {value!r}"
            )

        super().__init__(value=value)

class Days(_PositiveInt):
    ...


class DaysToKeep(_PositiveInt):
    ...


class ExecutionMillis(_PositiveInt):
    ...


class TimeoutSeconds(_PositiveInt):
    ...


class MaxRetries(_PositiveInt):
    ...


class FlexPercent(ValueObject):
    def __init__(self, value: float):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )
        elif isinstance(value, float):
            if value < 0 or value > 1:
                raise ValueError(
                    f"{self.__class__.__name__} value must be between 0 and 1, but got {value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects a float, but got {value!r}"
            )

        super().__init__(value=value)


class Timestamp(ValueObject):
    def __init__(self, value: datetime.datetime):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )

        if not isinstance(value, datetime.datetime):
            raise TypeError(
                f"{self.__class__.__name__} expects a datetime.datetime, but got {value!r}"
            )

        super().__init__(value=value)

    @classmethod
    def now(cls) -> Timestamp:
        return Timestamp(value=datetime.datetime.now())


class SMTPServer(_NonEmptyStr):
    ...


class SMTPPort(_PositiveInt):
    ...


class EmailAddress(ValueObject):
    def __init__(self, value: str):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )
        elif isinstance(value, str):
            if (
                re.match(r"^[a-zA-Z0-9]+[._]?[a-zA-Z0-9]+[@]\w+[.]\w{2,3}$", value)
                is None
            ):
                raise ValueError(f"{value!r} is not a valid EmailAddress.")
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects a str, but got {value!r}"
            )

        super().__init__(value=value)


class EmailSubject(ValueObject):
    def __init__(self, value: str):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )
        elif isinstance(value, str):
            if len(value) < 3 or len(value) > 200:
                raise ValueError(
                    f"{self.__class__.__name__} must be between 3 and 200 characters long, but got {value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects a str, but got {value!r}"
            )

        super().__init__(value=value)


class Password(ValueObject):
    def __init__(self, value: str):
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

        super().__init__(value=value)

    def __repr__(self) -> str:
        return f"Password({'*' * 10})"

    def __str__(self) -> str:
        return "*" * 10


class EmailMsg(_NonEmptyStr):
    ...


class LogLevelOption(enum.Enum):
    Debug = 1
    Info = 2
    Error = 3


class LogLevel(ValueObject):
    def __init__(self, value: LogLevelOption):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )

        if not isinstance(value, LogLevelOption):
            raise TypeError(
                f"{self.__class__.__name__} expects a LogLevelOption value, but got {value!r}."
            )

        super().__init__(value=value)

    @classmethod
    def debug(cls) -> LogLevel:
        return LogLevel(value=LogLevelOption.Debug)

    @classmethod
    def error(cls) -> LogLevel:
        return LogLevel(value=LogLevelOption.Error)

    @classmethod
    def info(cls) -> LogLevel:
        return LogLevel(value=LogLevelOption.Info)

    def __str__(self) -> str:
        if self.value == LogLevelOption.Debug:
            return "DEBUG"
        elif self.value == LogLevelOption.Info:
            return "INFO"
        elif self.value == LogLevelOption.Error:
            return "ERROR"
        else:
            raise ValueError(f"{self.value} is not a LogLevelOption value.")


class LogMessage(_NonEmptyStr):
    ...
