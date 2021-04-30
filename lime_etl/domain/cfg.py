import abc

from lime_etl.domain import value_objects

__all__ = ("Config",)


class Config(abc.ABC):
    @property
    @abc.abstractmethod
    def admin_engine_uri(self) -> value_objects.DbUri:
        raise NotImplementedError

    @property
    def admin_schema(self) -> value_objects.SchemaName:
        return value_objects.SchemaName("etl")

    @property
    def days_logs_to_keep(self) -> value_objects.DaysToKeep:
        return value_objects.DaysToKeep(3)
