import re
from typing import Any

import pytest

from lime_etl.services.admin.send_email_notifications import email_domain


@pytest.mark.parametrize(
    "cls, value",
    [
        (email_domain.EmailAddress, "test@gmail.com"),
        (email_domain.EmailMsg, "The is a test message."),
        (email_domain.EmailSubject, "Test Subject"),
        (email_domain.SMTPPort, 465),
        (email_domain.SMTPServer, "smtp.gmail.com"),
    ],
)
def test_value_object_accepts_values_of_correct_type(cls: type, value: Any) -> None:
    cls(value)


@pytest.mark.parametrize(
    "cls, value",
    [
        (email_domain.EmailAddress, 123),
        (email_domain.EmailMsg, 123),
        (email_domain.EmailSubject, 123),
        (email_domain.SMTPPort, "123"),
        (email_domain.SMTPServer, 123),
    ],
)
def test_value_object_rejects_values_of_incorrect_type(cls: type, value: Any) -> None:
    with pytest.raises(TypeError):
        cls(value)


@pytest.mark.parametrize(
    "cls",
    [
        email_domain.EmailAddress,
        email_domain.EmailMsg,
        email_domain.EmailSubject,
        email_domain.SMTPPort,
        email_domain.SMTPServer,
    ],
)
def test_value_object_rejects_none_value(cls: type) -> None:
    with pytest.raises(ValueError, match="value is required"):
        cls(None)


@pytest.mark.parametrize(
    "cls, value, expected_error_message",
    [
        (email_domain.EmailAddress, "test", "not a valid EmailAddress"),
        (
            email_domain.EmailSubject,
            "x",
            "EmailSubject must be between 3 and 200 characters",
        ),
        (email_domain.SMTPPort, -1, "SMTPPort value must be positive"),
        (email_domain.SMTPServer, "", "SMTPServer value is required"),
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
