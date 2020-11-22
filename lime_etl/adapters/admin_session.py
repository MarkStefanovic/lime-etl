from __future__ import annotations

import typing

import lime_uow as lu
from sqlalchemy import orm

__all__ = ("SqlAlchemyAdminSession",)


class SqlAlchemyAdminSession(lu.SqlAlchemySession):
    def __init__(self, session_factory: orm.sessionmaker):
        super().__init__(session_factory)

    @classmethod
    def interface(cls) -> typing.Type[SqlAlchemyAdminSession]:
        return SqlAlchemyAdminSession
