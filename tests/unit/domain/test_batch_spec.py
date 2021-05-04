import typing

import lime_uow as lu
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


class DummyJob(le.SimpleJobSpec):
    def __init__(self, name: str):
        super().__init__(name=name)

    def run(self, uow: lu.UnitOfWork, logger: le.JobLogger) -> le.JobStatus:
        return le.JobStatus.success()


class DummyResource(le.Resource[str]):
    def __init__(self, value: str):
        self.value = value

    def open(self, **kwargs: typing.Dict[str, typing.Any]) -> str:
        return self.value

    @staticmethod
    def key() -> str:
        return DummyResource.__name__


def test_minimal_create_batch_creates_valid_batch_spec() -> None:
    batch = le.create_batch(  # type: ignore
        name="test_batch",
        jobs=[DummyJob("dummy1"), DummyJob("dummy2")],
        create_uow=lambda cfg: DummyUoW(cfg),
    )
    assert batch.batch_name == le.BatchName("test_batch")
