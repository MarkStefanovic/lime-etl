from email.message import EmailMessage
from smtplib import SMTP
from typing import Protocol

from src.domain import msg, value_objects


class EmailAdapter(Protocol):
    def send(self, email: msg.Msg) -> None:
        ...


class DefaultEmailAdapter(EmailAdapter):
    def __init__(
        self,
        smtp_server: value_objects.SMTPServer,
        smtp_port: value_objects.SMTPPort,
        username: value_objects.EmailAddress,
        password: value_objects.Password,
        recipient: value_objects.EmailAddress,
    ):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.recipient = recipient

    def send(self, email: msg.Msg) -> None:
        print(f"Sending email; {email}")
        with SMTP(host=self.smtp_server.value, port=self.smtp_port.value) as s:
            s.login(user=self.username.value, password=self.password.value)
            msg = EmailMessage()
            msg.set_content(email.message.value)
            msg["Subject"] = email.subject.value
            msg["From"] = self.username.value
            msg["To"] = self.recipient.value
            s.send_message(msg)
            s.quit()
            return None
