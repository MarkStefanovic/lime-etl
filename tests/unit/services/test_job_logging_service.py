import pytest

from lime_etl.adapters import job_log_repository
from lime_etl.domain import value_objects
from lime_etl.services import job_logging_service
from tests import conftest


@pytest.fixture
def job_id() -> value_objects.UniqueId:
    return value_objects.UniqueId("b" * 32)


@pytest.fixture
def job_logger(
    dummy_uow: conftest.DummyUnitOfWork,
    dummy_job_log_entry_repository: job_log_repository.JobLogRepository,
) -> job_logging_service.DefaultJobLoggingService:
    return job_logging_service.DefaultJobLoggingService(
        uow=dummy_uow,
        batch_id=value_objects.UniqueId("a" * 32),
        job_id=value_objects.UniqueId("b" * 32),
    )


def test_job_logger_log_error(
    dummy_job_log_entry_repository: conftest.DummyJobLogRepository,
    dummy_uow: conftest.DummyUnitOfWork,
    job_logger: job_logging_service.JobLoggingService,
    job_id: value_objects.UniqueId,
) -> None:
    job_logger.log_error(
        message=value_objects.LogMessage("Test Message")
    )
    assert dummy_uow.committed
    first_entry = dummy_job_log_entry_repository.job_log[0]
    assert first_entry.log_level.value == value_objects.LogLevelOption.Error
    assert first_entry.message.value == "Test Message"


def test_job_logger_log_info(
    dummy_job_log_entry_repository: conftest.DummyJobLogRepository,
    dummy_uow: conftest.DummyUnitOfWork,
    job_logger: job_logging_service.DefaultJobLoggingService,
    job_id: value_objects.UniqueId,
) -> None:
    job_logger.log_info(
        message=value_objects.LogMessage("This is a test info message."),
    )
    assert dummy_uow.committed
    first_entry = dummy_job_log_entry_repository.job_log[0]
    assert first_entry.log_level.value == value_objects.LogLevelOption.Info
    assert first_entry.message.value == "This is a test info message."
