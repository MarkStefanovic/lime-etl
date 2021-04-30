import lime_etl as le


def test_minimal_create_job_creates_valid_job_spec():
    job = le.create_job(
        name="test_job",
        run=lambda uow, logger: le.JobStatus.success(),
    )
    assert job.job_name == le.JobName("test_job")
