import datetime

from src.domain import value_objects
from src.domain import job_test_result


def test_job_test_result_convertible_to_dto() -> None:
    test_result = job_test_result.JobTestResult(
        id=value_objects.UniqueId.generate(),
        job_id=value_objects.UniqueId.generate(),
        test_name=value_objects.TestName("test 1"),
        test_success_or_failure=value_objects.Result.success(),
        ts=value_objects.Timestamp(datetime.datetime(2010, 1, 1)),
    )
    test_result.to_dto()


def test_job_test_result_convertible_to_domain_object() -> None:
    test_result = job_test_result.JobTestResultDTO(
        id="abcd" * 8,
        job_id="abcd" * 8,
        test_name="test 1",
        test_passed=True,
        test_failure_message=None,
        ts=datetime.datetime(2010, 1, 1),
    )
    test_result.to_domain()
