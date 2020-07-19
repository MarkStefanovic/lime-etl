from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src import settings
from src.adapters import orm
from src.services import (
    batch_runner,
    unit_of_work,
)
from src.services.jobs import job_loader


def main() -> None:
    project_settings = settings.DotEnvSettings()
    orm.start_mappers()
    engine = create_engine(project_settings.db_uri)
    orm.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)

    uow = unit_of_work.DefaultUnitOfWork(
        session_factory=session_factory, project_settings=project_settings,
    )
    jobs = job_loader.load(project_settings=project_settings)

    result = batch_runner.run(uow=uow, jobs=jobs)
    print(f"{result=}")


if __name__ == "__main__":
    main()
