from __future__ import annotations

import typing

import lime_uow as lu
import sqlalchemy as sa
from sqlalchemy import orm

from lime_etl.adapters import admin_orm
from lime_etl.domain import value_objects


class SqlAlchemyAdminSession(lu.SqlAlchemySession):
    def __init__(self, session_factory: orm.sessionmaker):
        super().__init__(session_factory)

    @classmethod
    def interface(cls) -> typing.Type[SqlAlchemyAdminSession]:
        return SqlAlchemyAdminSession


def admin_session_factory(
    engine_or_uri: typing.Union[sa.engine.Engine, str],
    schema: typing.Optional[str],
) -> orm.sessionmaker:
    if schema:
        admin_orm.set_schema(schema=value_objects.SchemaName(schema))

    admin_orm.start_mappers()
    if type(engine_or_uri) is sa.engine.Engine:
        engine = typing.cast(sa.engine.Connectable, engine_or_uri)
    else:
        engine = sa.create_engine(engine_or_uri)
    admin_orm.metadata.create_all(engine)
    return orm.sessionmaker(bind=engine)
