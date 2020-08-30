from typing import List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from domain import job_spec
from services.jobs import delete_old_logs
from src import settings
from src.adapters import orm
from src.services import (
    batch_runner,
    unit_of_work,
)


def main() -> None:
    project_settings = settings.DotEnvSettings()
    orm.set_schema(schema=project_settings.etl_schema)
    orm.start_mappers()
    engine = create_engine(project_settings.db_uri, echo=True)
    # engine.execute("ATTACH DATABASE 'etl' AS etl;")
    orm.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)

    uow = unit_of_work.DefaultUnitOfWork(
        session_factory=session_factory, project_settings=project_settings,
    )

    admin: List[job_spec.JobSpec] = [
        delete_old_logs.DeleteOldLogs(
            days_to_keep=project_settings.days_of_logs_to_keep
        ),
    ]
    etl_jobs: List[job_spec.JobSpec] = []

    result = batch_runner.run(uow=uow, jobs=admin + etl_jobs)
    print(f"{result=}")


if __name__ == "__main__":
    main()
