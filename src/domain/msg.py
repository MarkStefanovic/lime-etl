from __future__ import annotations

import hashlib
from dataclasses import dataclass

from src.domain import value_objects


@dataclass(eq=True, unsafe_hash=True)
class MsgDTO:
    subject: str
    message: str

    def to_domain(self) -> Msg:
        return Msg(
            subject=value_objects.EmailSubject(self.subject),
            message=value_objects.EmailMsg(self.message),
        )


@dataclass(frozen=True)
class Msg:
    subject: value_objects.EmailSubject
    message: value_objects.EmailMsg

    # noinspection InsecureHash
    @property
    def message_hash(self) -> str:
        return hashlib.md5(bytes(self.message.value, encoding="utf-8")).hexdigest()

    def to_dto(self) -> MsgDTO:
        return MsgDTO(subject=self.subject.value, message=self.message.value,)
