import abc
import datetime
from typing import List, Optional

from sqlalchemy import desc
from sqlalchemy.orm import Session

from src.adapters import timestamp_adapter
from src.domain import batch, job_result, job_test_result, value_objects


class BatchRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, new_batch: batch.Batch) -> batch.Batch:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_old_entries(
        self, days_to_keep: value_objects.DaysToKeep
    ) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def get_latest(self) -> Optional[batch.Batch]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_latest_test_results_for_job(
        self, job_name: value_objects.JobName
    ) -> List[job_test_result.JobTestResult]:
        raise NotImplementedError


class SqlAlchemyBatchRepository(BatchRepository):
    def __init__(
        self, session: Session, ts_adapter: timestamp_adapter.TimestampAdapter
    ):
        self._session = session
        self._ts_adapter = ts_adapter
        super().__init__()

    def add(self, new_batch: batch.Batch) -> batch.Batch:
        dto = new_batch.to_dto()
        self._session.add(dto)
        return new_batch

    def delete_old_entries(
        self, days_to_keep: value_objects.DaysToKeep,
    ) -> int:
        ts = self._ts_adapter.now().value
        cutoff: datetime.datetime = ts - datetime.timedelta(
            days=days_to_keep.value
        )
        # We need to delete batches one by one to trigger cascade deletes, a bulk update will
        # not trigger them, and we don't want to rely on specific database implementations, so
        # we cannot use ondelete='CASCADE' on the foreign key columns.
        batches: List[batch.BatchDTO] = (
            self._session.query(batch.BatchDTO)
            .filter(batch.BatchDTO.ts < cutoff)
            .all()
        )
        for b in batches:
            self._session.delete(b)
        return len(batches)

    def get_latest(self) -> Optional[batch.Batch]:
        result: Optional[batch.BatchDTO] = (
            self._session.query(batch.BatchDTO)
            .order_by(desc(batch.BatchDTO.ts))
            .first()
        )
        if result is None:
            return None
        else:
            return result.to_domain()

    def get_latest_test_results_for_job(
        self, job_name: value_objects.JobName
    ) -> List[job_test_result.JobTestResult]:
        jr: Optional[job_result.JobResultDTO] = (
            self._session.query(job_result.JobResultDTO)
            .filter(job_result.JobResultDTO.job_name.like(job_name.value))  # type: ignore
            .order_by(desc(job_result.JobResultDTO.ts))
            .first()
        )
        if jr is None:
            return []
        else:
            return [tr.to_domain() for tr in jr.test_results]
