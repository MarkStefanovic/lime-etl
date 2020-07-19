import pytest

from src.adapters import batch_log_repository
from src.domain import value_objects
from src.services import batch_logging_service
from tests import conftest


@pytest.fixture
def batch_logger(
    dummy_batch_log_entry_repository: batch_log_repository.BatchLogRepository,
) -> batch_logging_service.DefaultBatchLoggingService:
    return batch_logging_service.DefaultBatchLoggingService(
        batch_id=value_objects.UniqueId("a" * 32)
    )


def test_batch_logger_log_error(
    dummy_batch_log_entry_repository: conftest.DummyBatchLogRepository,
    dummy_uow: conftest.DummyUnitOfWork,
    batch_logger: batch_logging_service.BatchLoggingService,
) -> None:
    batch_logger.log_error(
        uow=dummy_uow, message=value_objects.LogMessage("Test Message")
    )
    assert dummy_uow.committed
    first_entry = dummy_batch_log_entry_repository.batch_log[0]
    assert first_entry.log_level.value == value_objects.LogLevelOption.Error
    assert first_entry.message.value == "Test Message"


def test_batch_logger_log_info(
    dummy_batch_log_entry_repository: conftest.DummyBatchLogRepository,
    dummy_uow: conftest.DummyUnitOfWork,
    batch_logger: batch_logging_service.BatchLoggingService,
) -> None:
    batch_logger.log_info(
        uow=dummy_uow, message=value_objects.LogMessage("This is a test info message.")
    )
    assert dummy_uow.committed
    first_entry = dummy_batch_log_entry_repository.batch_log[0]
    assert first_entry.log_level.value == value_objects.LogLevelOption.Info
    assert first_entry.message.value == "This is a test info message."


def test_batch_logger_log_start(
    dummy_batch_log_entry_repository: conftest.DummyBatchLogRepository,
    dummy_uow: conftest.DummyUnitOfWork,
    batch_logger: batch_logging_service.BatchLoggingService,
) -> None:
    batch_logger.log_start(uow=dummy_uow)
    assert dummy_uow.committed
    first_entry = dummy_batch_log_entry_repository.batch_log[0]
    assert first_entry.log_level.value == value_objects.LogLevelOption.Info
    assert first_entry.message.value == "Batch started."


def test_batch_logger_log_completed_successfully(
    dummy_batch_log_entry_repository: conftest.DummyBatchLogRepository,
    dummy_uow: conftest.DummyUnitOfWork,
    batch_logger: batch_logging_service.BatchLoggingService,
) -> None:
    batch_logger.log_completed_successfully(uow=dummy_uow)
    assert dummy_uow.committed
    first_entry = dummy_batch_log_entry_repository.batch_log[0]
    assert first_entry.log_level.value == value_objects.LogLevelOption.Info
    assert first_entry.message.value == "Batch completed successfully."
