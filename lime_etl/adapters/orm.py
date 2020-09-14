from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
)
from sqlalchemy.orm import mapper, relationship

from domain import (
    batch,
    job_result,
    job_log_entry,
    job_test_result,
    batch_log_entry,
    value_objects,
)

metadata = MetaData()


# @event.listens_for(Engine, "connect")
# def set_sqlite_pragma(dbapi_connection, _):
#     cursor = dbapi_connection.cursor()
#     cursor.execute("PRAGMA foreign_keys=ON")
#     cursor.close()


batches = Table(
    "batches",
    metadata,
    Column("id", String(32), primary_key=True),
    Column("execution_millis", Integer, nullable=False),
    Column("execution_error_occurred", Boolean, nullable=False),
    Column("execution_error_message", String(2000), nullable=True),
    Column("ts", DateTime, nullable=False),
)

batch_log = Table(
    "batch_log",
    metadata,
    Column("id", String(32), primary_key=True),
    Column("batch_id", String(32), nullable=False),
    Column("log_level", Enum(value_objects.LogLevelOption)),
    Column("message", String(2000), nullable=False),
    Column("ts", DateTime, nullable=False),
)

job_log = Table(
    "job_log",
    metadata,
    Column("id", String(32), primary_key=True),
    Column("batch_id", String(32), nullable=False),
    Column("job_id", String(32), nullable=False),
    Column("log_level", Enum(value_objects.LogLevelOption)),
    Column("message", String(2000), nullable=False),
    Column("ts", DateTime, nullable=False),
)

jobs = Table(
    "admin",
    metadata,
    Column("id", String(32), primary_key=True),
    Column("batch_id", ForeignKey("batches.id")),
    Column("job_name", String(200), nullable=False),
    Column("execution_millis", Integer, nullable=False),
    Column("execution_error_occurred", Boolean, nullable=False),
    Column("execution_error_message", String(2000), nullable=True),
    Column("ts", DateTime, nullable=False),
)

job_test_results = Table(
    "job_test_results",
    metadata,
    Column("id", String(32), primary_key=True),
    Column("job_id", ForeignKey("admin.id")),
    Column("test_name", String(100), nullable=False),
    Column("test_passed", Boolean, nullable=True),
    Column("test_failure_message", String(2000), nullable=True),
    Column("ts", DateTime, nullable=False),
)


def set_schema(schema: value_objects.SchemaName) -> None:
    for table_name, table in metadata.tables.items():
        table.schema = schema.value


def start_mappers() -> None:
    mapper(batch_log_entry.BatchLogEntryDTO, batch_log)
    mapper(job_log_entry.JobLogEntryDTO, job_log)
    job_test_result_mapper = mapper(job_test_result.JobTestResultDTO, job_test_results)
    job_mapper = mapper(
        job_result.JobResultDTO,
        jobs,
        properties={
            "test_results": relationship(
                job_test_result_mapper,
                cascade="all,delete,delete-orphan",
                collection_class=list,
            ),
        },
    )
    mapper(
        batch.BatchDTO,
        batches,
        properties={
            "job_results": relationship(
                job_mapper, cascade="all,delete,delete-orphan", collection_class=list,
            ),
        },
    )