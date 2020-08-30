import abc
import os

from dotenv import find_dotenv, load_dotenv

from src.domain import value_objects


class Settings(abc.ABC):
    @property
    @abc.abstractmethod
    def days_of_logs_to_keep(self) -> value_objects.DaysToKeep:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def db_uri(self) -> str:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def email_recipient(self) -> value_objects.EmailAddress:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def email_username(self) -> value_objects.EmailAddress:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def email_password(self) -> value_objects.Password:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def etl_schema(self) -> value_objects.SchemaName:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def smtp_server(self) -> value_objects.SMTPServer:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def smtp_port(self) -> value_objects.SMTPPort:
        raise NotImplementedError


class DotEnvSettings(Settings):
    def __init__(self) -> None:
        load_dotenv(dotenv_path=find_dotenv())

    @property
    def days_of_logs_to_keep(self) -> value_objects.DaysToKeep:
        return value_objects.DaysToKeep(int(os.environ["DAYS_OF_LOGS_TO_KEEP"]))

    @property
    def db_uri(self) -> str:
        return os.environ["DB_URI"]

    @property
    def email_recipient(self) -> value_objects.EmailAddress:
        return value_objects.EmailAddress(os.environ["EMAIL_ON_ERROR"])

    @property
    def email_username(self) -> value_objects.EmailAddress:
        return value_objects.EmailAddress(os.environ["EMAIL_USERNAME"])

    @property
    def email_password(self) -> value_objects.Password:
        return value_objects.Password(os.environ["EMAIL_PASSWORD"])

    @property
    def etl_schema(self) -> value_objects.SchemaName:
        return value_objects.SchemaName(os.environ["ETL_SCHEMA"] or None)

    @property
    def smtp_server(self) -> value_objects.SMTPServer:
        return value_objects.SMTPServer(os.environ["SMTP_SERVER"])

    @property
    def smtp_port(self) -> value_objects.SMTPPort:
        return value_objects.SMTPPort(int(os.environ["SMTP_PORT"]))
