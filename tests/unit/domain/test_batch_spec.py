import typing

import lime_etl as le


class DummyUoW(le.UnitOfWork):
    def __init__(self, config: le.Config):
        super().__init__()
        self._config = config

    def create_resources(
        self, /, shared_resources: le.SharedResourceManager
    ) -> typing.List[le.Resource[typing.Any]]:
        return [DummyResource("test_resource_1")]

    def create_shared_resources(self) -> typing.List[le.Resource[typing.Any]]:
        return [DummyResource("test_shared_resource_1")]


class DummyResource(le.Resource[str]):
    def __init__(self, value: str):
        self.value = value

    def open(self, **kwargs: typing.Dict[str, typing.Any]) -> str:
        return self.value

    @staticmethod
    def key() -> str:
        return DummyResource.__name__


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
