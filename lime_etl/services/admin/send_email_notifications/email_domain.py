import re

from lime_etl.domain import value_objects


class SMTPServer(value_objects._NonEmptyStr):
    ...


class SMTPPort(value_objects._PositiveInt):
    ...


class EmailMsg(value_objects._NonEmptyStr):
    ...


class EmailAddress(value_objects.ValueObject):
    def __init__(self, value: str, /):
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

        super().__init__(value)


class EmailSubject(value_objects.ValueObject):
    def __init__(self, value: str, /):
        if value is None:
            raise ValueError(
                f"{self.__class__.__name__} value is required, but got None."
            )
        elif isinstance(value, str):
            if len(value) < 3 or len(value) > 200:
                raise ValueError(
                    f"{self.__class__.__name__} must be between 3 and 200 characters long, but "
                    f"got {value!r}."
                )
        else:
            raise TypeError(
                f"{self.__class__.__name__} expects a str, but got {value!r}"
            )

        super().__init__(value)
