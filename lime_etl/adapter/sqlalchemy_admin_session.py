from __future__ import annotations

from lime_uow import sqlalchemy_resources as lsa
from sqlalchemy import orm

__all__ = ("SqlAlchemyAdminSession",)


class SqlAlchemyAdminSession(lsa.SqlAlchemySession):
    def __init__(self, session_factory: orm.sessionmaker):
        super().__init__(session_factory)

    @staticmethod
    def key() -> str:
        return SqlAlchemyAdminSession.__name__
