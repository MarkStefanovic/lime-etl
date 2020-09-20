import datetime
import re
from typing import Any

import pytest

from lime_etl.domain import value_objects


@pytest.mark.parametrize(
    "cls, value",
    [
        (value_objects.Days, 1),
        (value_objects.EmailAddress, "test@gmail.com"),
        (value_objects.EmailMsg, "The is a test message."),
        (value_objects.EmailSubject, "Test Subject"),
        (value_objects.ExecutionMillis, 10),
        (value_objects.Flag, True),
        (value_objects.FlexPercent, 0.98),
        (value_objects.JobName, "Test Job"),
        (value_objects.LogLevel, value_objects.LogLevelOption.Info),
        (value_objects.LogMessage, "This is a test message."),
        (value_objects.MaxRetries, 3),
        (value_objects.Password, "password"),
        (value_objects.Result, value_objects.Failure("Failed.")),
        (value_objects.SchemaName, "manager"),
        (value_objects.SecondsBetweenRefreshes, 300),
        (value_objects.SingleChar, "T"),
        (value_objects.SMTPPort, 465),
        (value_objects.SMTPServer, "smtp.gmail.com"),
        (value_objects.TestName, "Customer numbers are unique"),
        (value_objects.TimeoutSeconds, 120),
        (value_objects.Timestamp, datetime.datetime(2010, 1, 2, 3, 4, 5)),
        (value_objects.UniqueId, "a" * 32),
    ],
)
def test_value_object_accepts_values_of_correct_type(cls: type, value: Any) -> None:
    cls(value)


@pytest.mark.parametrize(
    "cls, value",
    [
        (value_objects.Days, "123"),
        (value_objects.EmailAddress, 123),
        (value_objects.EmailMsg, 123),
        (value_objects.EmailSubject, 123),
        (value_objects.ExecutionMillis, "abc"),
        (value_objects.Flag, "true"),
        (value_objects.FlexPercent, "0"),
        (value_objects.JobName, 123),
        (value_objects.LogLevel, "info"),
        (value_objects.LogMessage, 123),
        (value_objects.MaxRetries, "3"),
        (value_objects.Password, 123),
        (value_objects.Result, "success"),
        (value_objects.SchemaName, 123),
        (value_objects.SecondsBetweenRefreshes, "999"),
        (value_objects.SingleChar, 1),
        (value_objects.SMTPPort, "123"),
        (value_objects.SMTPServer, 123),
        (value_objects.TestName, 123),
        (value_objects.TimeoutSeconds, "120"),
        (value_objects.Timestamp, "2010-01-02"),
        (value_objects.UniqueId, 123),
    ],
)
def test_value_object_rejects_values_of_incorrect_type(cls: type, value: Any) -> None:
    with pytest.raises(TypeError):
        cls(value)


@pytest.mark.parametrize(
    "cls",
    [
        value_objects.Days,
        value_objects.EmailAddress,
        value_objects.EmailMsg,
        value_objects.EmailSubject,
        value_objects.ExecutionMillis,
        value_objects.Flag,
        value_objects.FlexPercent,
        value_objects.JobName,
        value_objects.LogLevel,
        value_objects.LogMessage,
        value_objects.MaxRetries,
        value_objects.Password,
        value_objects.Result,
        value_objects.SecondsBetweenRefreshes,
        value_objects.SingleChar,
        value_objects.SMTPPort,
        value_objects.SMTPServer,
        value_objects.TestName,
        value_objects.TimeoutSeconds,
        value_objects.Timestamp,
        value_objects.UniqueId,
    ],
)
def test_value_object_rejects_none_value(cls: type) -> None:
    with pytest.raises(ValueError, match="value is required"):
        cls(None)


@pytest.mark.parametrize(
    "cls, value, expected_error_message",
    [
        (value_objects.Days, -1, "days value must be positive"),
        (value_objects.EmailAddress, "test", "not a valid EmailAddress"),
        (
            value_objects.EmailSubject,
            "x",
            "EmailSubject must be between 3 and 200 characters",
        ),
        (value_objects.ExecutionMillis, -1, "ExecutionMillis value must be positive"),
        (value_objects.FlexPercent, -1.2, "FlexPercent value must be between 0 and 1"),
        (value_objects.JobName, "a", "JobName must be between 3 and 100 characters"),
        (value_objects.MaxRetries, -1, "MaxRetries value must be positive"),
        (value_objects.Password, "", "Password value is required"),
        (value_objects.SchemaName, "", "If a SchemaName value is provided, then it must be at least 1 character long"),
        (value_objects.SecondsBetweenRefreshes, 1, "must be at least 300 seconds"),
        (value_objects.SingleChar, "", "SingleChar value is required"),
        (value_objects.SingleChar, "abc", "SingleChar must be 1 char"),
        (value_objects.SMTPPort, -1, "SMTPPort value must be positive"),
        (value_objects.SMTPServer, "", "SMTPServer value is required"),
        (
            value_objects.TestName,
            "",
            "TestName must be between 3 and 100 characters long",
        ),
        (value_objects.TimeoutSeconds, -1, "TimeoutSeconds value must be positive"),
        (value_objects.UniqueId, "a" * 33, "must be 32 characters"),
        (value_objects.UniqueId, "a", "must be 32 characters"),
    ],
)
def test_value_object_rejects_values_of_that_are_out_of_bounds(
    cls: type, value: Any, expected_error_message: str
) -> None:
    # with pytest.raises(ValueError, match=expected_error_message) as e:
    with pytest.raises(ValueError) as e:
        cls(value)
    assert e.match(
        re.compile(pattern=expected_error_message, flags=re.RegexFlag.IGNORECASE)
    )


def test_unique_id_generates_valid_uuid() -> None:
    uuid = value_objects.UniqueId.generate()
    assert len(uuid.value) == 32


def test_value_object__eq__() -> None:
    v = value_objects.ValueObject(1)
    v2 = value_objects.ValueObject(1)
    assert v.__eq__(v2)


def test_value_object__ge__() -> None:
    v = value_objects.ValueObject(2)
    v2 = value_objects.ValueObject(1)
    assert v >= v2


def test_value_object__gt__() -> None:
    v = value_objects.ValueObject(2)
    v2 = value_objects.ValueObject(1)
    assert v > v2


def test_value_object__hash__() -> None:
    v = value_objects.ValueObject(1)
    assert hash(v) == hash(1)


def test_value_object__le__() -> None:
    v = value_objects.ValueObject(1)
    v2 = value_objects.ValueObject(2)
    assert v <= v2


def test_value_object__lt__() -> None:
    v = value_objects.ValueObject(1)
    v2 = value_objects.ValueObject(2)
    assert v < v2


def test_value_object__ne__() -> None:
    v = value_objects.ValueObject(1)
    v2 = value_objects.ValueObject(2)
    assert v != v2


def test_value_object__repr__() -> None:
    v = value_objects.ValueObject(1)
    assert repr(v) == "ValueObject(1)"
