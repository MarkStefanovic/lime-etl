import dataclasses
import datetime
import typing

from lime_etl.domain import batch_status, job_result, job_test_result, value_objects


def describe_id(id_value: value_objects.UniqueId, /) -> str:
    if id_value.value is None:
        return "none"
    else:
        return f"{len(id_value.value)} chars"


def describe_ts(ts: value_objects.Timestamp, /) -> str:
    if ts.value is None:
        return "none"
    elif ts.value > datetime.datetime.now() + datetime.timedelta(days=1):
        return "future"
    elif ts.value < datetime.datetime.now() - datetime.timedelta(days=1):
        return "past"
    else:
        return "present"


def describe_execution_millis(millis: typing.Optional[value_objects.ExecutionMillis], /) -> str:
    if millis is None:
        return "none"
    elif millis.value >= 0:
        return "positive"
    else:
        return "negative"


def job_test_result_to_deterministic_dict(
    result: job_test_result.JobTestResult,
) -> typing.Dict[str, typing.Any]:
    d = dataclasses.asdict(result.to_dto())
    d["id"] = describe_id(result.id)
    d["job_id"] = describe_id(result.job_id)
    d["execution_millis"] = describe_execution_millis(result.execution_millis)
    d["ts"] = describe_ts(result.ts)
    return d


def job_result_to_deterministic_dict(
    result: job_result.JobResult,
) -> typing.Dict[str, typing.Any]:
    d = dataclasses.asdict(result.to_dto())
    d["id"] = describe_id(result.id)
    d["batch_id"] = describe_id(result.batch_id)
    d["execution_millis"] = describe_execution_millis(result.execution_millis)
    d["ts"] = describe_ts(result.ts)
    d["test_results"] = [
        job_test_result_to_deterministic_dict(r)
        for r in sorted(result.test_results, key=lambda j: j.test_name.value)
    ]
    return d


def batch_result_to_deterministic_dict(
    result: batch_status.BatchStatus,
) -> typing.Dict[str, typing.Any]:
    d = dataclasses.asdict(result.to_dto())
    d["id"] = describe_id(result.id)
    d["execution_millis"] = describe_execution_millis(result.execution_millis)
    d["ts"] = describe_ts(result.ts)
    d["job_results"] = [
        job_result_to_deterministic_dict(r)
        for r in sorted(result.job_results, key=lambda b: b.job_name.value)
    ]
    return d
