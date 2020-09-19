import abc
import datetime
from typing import List, Optional

from sqlalchemy import desc
from sqlalchemy.orm import Session

from adapters import timestamp_adapter  # type: ignore
from domain import batch, job_result, job_test_result, value_objects  # type: ignore


class BatchRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, new_batch: batch.Batch) -> batch.Batch:
        raise NotImplementedError

    @abc.abstractmethod
    def add_job_result(self, result: job_result.JobResult) -> job_result.JobResult:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_old_entries(self, days_to_keep: value_objects.DaysToKeep) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def get_batch_by_id(self, batch_id: value_objects.UniqueId) -> Optional[batch.Batch]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_latest(self) -> Optional[batch.Batch]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_latest_results_for_job(
        self, job_name: value_objects.JobName
    ) -> List[job_result.JobResult]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_last_successful_ts_for_job(
        self, job_name: value_objects.JobName
    ) -> Optional[value_objects.Timestamp]:
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, batch_to_update: batch.Batch) -> batch.Batch:
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

    def add_job_result(self, result: job_result.JobResult) -> job_result.JobResult:
        dto = result.to_dto()
        self._session.add(dto)
        return result

    def delete_old_entries(
        self,
        days_to_keep: value_objects.DaysToKeep,
    ) -> int:
        ts = self._ts_adapter.now().value
        cutoff: datetime.datetime = ts - datetime.timedelta(days=days_to_keep.value)
        # We need to delete batches one by one to trigger cascade deletes, a bulk update will
        # not trigger them, and we don't want to rely on specific database implementations, so
        # we cannot use ondelete='CASCADE' on the foreign key columns.
        batches: List[batch.BatchDTO] = (
            self._session.query(batch.BatchDTO).filter(batch.BatchDTO.ts < cutoff).all()
        )
        for b in batches:
            self._session.delete(b)
        return len(batches)

    def get_batch_by_id(self, batch_id: value_objects.UniqueId) -> Optional[batch.Batch]:
        result: Optional[batch.BatchDTO] = (
            self._session.query(batch.BatchDTO)
            .filter(batch.BatchDTO.id == batch_id.value)
            .first()
        )
        if result is None:
            return None
        else:
            return result.to_domain()

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

    def get_latest_results_for_job(
        self, job_name: value_objects.JobName
    ) -> Optional[job_result.JobResult]:
        dto = (
            self._session.query(job_result.JobResultDTO)
            .filter(job_result.JobResultDTO.job_name.ilike(job_name.value))
            .order_by(desc(job_result.JobResultDTO.ts))
            .first()
        )
        if dto is None:
            return None
        else:
            return dto.to_domain()

    def get_last_successful_ts_for_job(
        self, job_name: value_objects.JobName
    ) -> Optional[value_objects.Timestamp]:
        jr: Optional[job_result.JobResultDTO] = (
            self._session.query(job_result.JobResultDTO)
            .filter(job_result.JobResultDTO.job_name.ilike(job_name.value))
            .filter(job_result.JobResultDTO.execution_error_occurred.is_(False))
            .order_by(desc(job_result.JobResultDTO.ts))
            .first()
        )
        if jr is None:
            return None
        else:
            return value_objects.Timestamp(jr.ts)

    def update(self, batch_to_update: batch.Batch) -> batch.Batch:
        dto = batch_to_update.to_dto()
        self._session.merge(dto)
        return batch_to_update
