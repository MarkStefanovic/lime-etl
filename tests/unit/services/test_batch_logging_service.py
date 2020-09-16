import pytest

from adapters import batch_log_repository  # type: ignore
from domain import value_objects  # type: ignore
from services import batch_logging_service  # type: ignore
from tests import conftest


@pytest.fixture
def batch_logger(
    dummy_uow: conftest.DummyUnitOfWork,
    dummy_batch_log_entry_repository: batch_log_repository.BatchLogRepository,
) -> batch_logging_service.DefaultBatchLoggingService:
    return batch_logging_service.DefaultBatchLoggingService(
        uow=dummy_uow, batch_id=value_objects.UniqueId("a" * 32)
    )


def test_batch_logger_log_error(
    dummy_batch_log_entry_repository: conftest.DummyBatchLogRepository,
    dummy_uow: conftest.DummyUnitOfWork,
    batch_logger: batch_logging_service.BatchLoggingService,
) -> None:
    batch_logger.log_error(
        message=value_objects.LogMessage("Test Message")
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
        message=value_objects.LogMessage("This is a test info message.")
    )
    assert dummy_uow.committed
    first_entry = dummy_batch_log_entry_repository.batch_log[0]
    assert first_entry.log_level.value == value_objects.LogLevelOption.Info
    assert first_entry.message.value == "This is a test info message."
