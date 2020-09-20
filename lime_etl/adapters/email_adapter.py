import typing
from email.message import EmailMessage
from smtplib import SMTP

from lime_etl.domain import batch_delta, value_objects


class EmailAdapter(typing.Protocol):
    def send(self, result: batch_delta.BatchDelta) -> None:
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

    def send(self, result: batch_delta.BatchDelta) -> None:
        if result.current_results.broken_jobs:
            if result.newly_broken_jobs:
                subject: typing.Optional[value_objects.EmailSubject] = value_objects.EmailSubject("ETL STILL BROKEN")
            else:
                subject = value_objects.EmailSubject("ETL BROKEN")
        else:
            if result.newly_fixed_jobs:
                subject = value_objects.EmailSubject("ETL FIXED")
            else:
                subject = None

        if subject:
            email_msg = value_objects.EmailMsg(
                "broken jobs: {}".format(
                    ", ".join(jn.value for jn in result.current_results.broken_jobs)
                )
                + "newly broken jobs: {}".format(
                    ", ".join(jn.value for jn in result.newly_broken_jobs)
                )
                + "newly fixed jobs: {}".format(
                    ", ".join(jn.value for jn in result.newly_fixed_jobs)
                )
            )
            with SMTP(host=self.smtp_server.value, port=self.smtp_port.value) as s:
                s.login(user=self.username.value, password=self.password.value)
                msg = EmailMessage()
                msg.set_content(email_msg.value)
                msg["Subject"] = subject.value
                msg["From"] = self.username.value
                msg["To"] = self.recipient.value
                s.send_message(msg)
                s.quit()
                return None
        else:
            return None
