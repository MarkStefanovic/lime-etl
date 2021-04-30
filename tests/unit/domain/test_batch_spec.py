import typing

import lime_etl as le


class DummyUoW(le.UnitOfWork):
    def __init__(self, config: typing.Dict[str, typing.Any]):
        super().__init__()
        self._config = config

    def create_resources(
        self, /, shared_resources: le.SharedResourceManager
    ) -> typing.List[le.Resource[typing.Any]]:
        return []

    def create_shared_resources(self) -> typing.List[le.Resource[typing.Any]]:
        return []


def test_minimal_create_batch_creates_valid_batch_spec() -> None:
    def dummy_job(name: str) -> le.JobSpec[DummyUoW]:
        return le.create_job(
            name=name,
            run=lambda uow, logger: le.JobStatus.success(),
        )

    def create_dummy_runtime_jobs(_: DummyUoW) -> typing.List[le.JobSpec[DummyUoW]]:
        return [dummy_job(name) for name in ("dummy3", "dummy4")]

    batch = le.create_batch(  # type: ignore
        name="test_batch",
        jobs=[dummy_job("dummy1"), dummy_job("dummy2")],
        runtime_jobs=create_dummy_runtime_jobs,
        create_uow=lambda cfg: DummyUoW(cfg),
    )
    assert batch.batch_name == le.BatchName("test_batch")