from __future__ import annotations

import typing

from lime_uow import sqlalchemy_resources as lsa
from sqlalchemy import orm

__all__ = ("SqlAlchemyAdminSession",)


class SqlAlchemyAdminSession(lsa.SqlAlchemySession):
    def __init__(self, session_factory: orm.sessionmaker):
        super().__init__(session_factory)

    @classmethod
    def interface(cls) -> typing.Type[SqlAlchemyAdminSession]:
        return SqlAlchemyAdminSession
