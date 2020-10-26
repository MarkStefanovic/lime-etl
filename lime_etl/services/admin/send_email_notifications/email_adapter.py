import typing
from email.message import EmailMessage
from smtplib import SMTP

from lime_etl.domain import value_objects
from lime_etl.services.admin.send_email_notifications import batch_delta
from lime_etl.services.admin.send_email_notifications import email_domain


class EmailAdapter(typing.Protocol):
    def send(self, result: batch_delta.BatchDelta) -> None:
        ...


class DefaultEmailAdapter(EmailAdapter):
    def __init__(
        self,
        smtp_server: email_domain.SMTPServer,
        smtp_port: email_domain.SMTPPort,
        username: email_domain.EmailAddress,
        password: value_objects.Password,
        recipient: email_domain.EmailAddress,
    ):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.recipient = recipient

    def send(self, result: batch_delta.BatchDelta) -> None:
        if result.current_results.broken_jobs:
            if result.newly_broken_jobs:
                subject: typing.Optional[email_domain.EmailSubject] = email_domain.EmailSubject("ETL STILL BROKEN")
            else:
                subject = email_domain.EmailSubject("ETL BROKEN")
        else:
            if result.newly_fixed_jobs:
                subject = email_domain.EmailSubject("ETL FIXED")
            else:
                subject = None

        if subject:
            email_msg = email_domain.EmailMsg(
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
