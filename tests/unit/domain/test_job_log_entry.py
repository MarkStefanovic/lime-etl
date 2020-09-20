from lime_etl.domain import job_log_entry, value_objects


def test_job_log_entry__str__() -> None:
    entry = job_log_entry.JobLogEntry(
        id=value_objects.UniqueId.generate(),
        batch_id=value_objects.UniqueId.generate(),
        job_id=value_objects.UniqueId.generate(),
        log_level=value_objects.LogLevel.error(),
        message=value_objects.LogMessage("Danger Will Robinson!"),
        ts=value_objects.Timestamp.now(),
    )
    assert "Danger Will Robinson!" in str(entry)
