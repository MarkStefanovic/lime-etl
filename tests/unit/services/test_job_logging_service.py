import pytest

from src.adapters import job_log_repository
from src.domain import value_objects
from src.services import job_logging_service
from tests import conftest


@pytest.fixture
def job_id() -> value_objects.UniqueId:
    return value_objects.UniqueId("b" * 32)


@pytest.fixture
def job_logger(
    dummy_job_log_entry_repository: job_log_repository.JobLogRepository,
) -> job_logging_service.DefaultJobLoggingService:
    return job_logging_service.DefaultJobLoggingService(
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
        uow=dummy_uow, message=value_objects.LogMessage("Test Message")
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
        uow=dummy_uow, message=value_objects.LogMessage("This is a test info message."),
    )
    assert dummy_uow.committed
    first_entry = dummy_job_log_entry_repository.job_log[0]
    assert first_entry.log_level.value == value_objects.LogLevelOption.Info
    assert first_entry.message.value == "This is a test info message."


def test_job_logger_log_start(
    dummy_job_log_entry_repository: conftest.DummyJobLogRepository,
    dummy_uow: conftest.DummyUnitOfWork,
    job_logger: job_logging_service.DefaultJobLoggingService,
    job_id: value_objects.UniqueId,
) -> None:
    job_logger.log_start(uow=dummy_uow)
    assert dummy_uow.committed
    first_entry = dummy_job_log_entry_repository.job_log[0]
    assert first_entry.log_level.value == value_objects.LogLevelOption.Info
    assert first_entry.message.value == "Job started."


def test_job_logger_log_completed_successfully(
    dummy_job_log_entry_repository: conftest.DummyJobLogRepository,
    dummy_uow: conftest.DummyUnitOfWork,
    job_logger: job_logging_service.DefaultJobLoggingService,
    job_id: value_objects.UniqueId,
) -> None:
    job_logger.log_completed_successfully(uow=dummy_uow)
    assert dummy_uow.committed
    first_entry = dummy_job_log_entry_repository.job_log[0]
    assert first_entry.log_level.value == value_objects.LogLevelOption.Info
    assert first_entry.message.value == "Job completed successfully."


def test_job_logger_log_retry(
    dummy_job_log_entry_repository: conftest.DummyJobLogRepository,
    dummy_uow: conftest.DummyUnitOfWork,
    job_logger: job_logging_service.DefaultJobLoggingService,
    job_id: value_objects.UniqueId,
) -> None:
    job_logger.log_retry(uow=dummy_uow, retry=1, max_retries=3)
    assert dummy_uow.committed
    first_entry = dummy_job_log_entry_repository.job_log[0]
    assert first_entry.log_level.value == value_objects.LogLevelOption.Info
    assert first_entry.message.value == "Running retry 1 of 3..."
