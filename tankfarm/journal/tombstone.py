"""Rollback expressed as an appended tombstone record."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tankfarm.errors import (
    DuplicateTombstoneError,
    RollbackTargetError,
    UncommittedRecordError,
)
from tankfarm.journal.commit import CommitController
from tankfarm.journal.record import Record

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from tankfarm.journal.writer import JournalWriter

TOMBSTONE_KIND = "journal.tombstone"


class TombstoneIndex:
    """Remembers which record sequences were rolled back."""

    def __init__(self) -> None:
        self._targets: set[int] = set()

    def mark(self, seq: int) -> bool:
        if seq in self._targets:
            return False
        self._targets.add(seq)
        return True

    def is_tombstoned(self, seq: int) -> bool:
        return seq in self._targets

    def targets(self) -> tuple[int, ...]:
        return tuple(sorted(self._targets))


class RollbackService:
    """Rolls a committed record back without rewriting history."""

    def __init__(
        self, journal: "JournalWriter", commits: CommitController, index: TombstoneIndex
    ) -> None:
        self._journal = journal
        self._commits = commits
        self._index = index

    def rollback(self, seq: int, reason: str) -> Record:
        self._journal.stream.get(seq)
        if not self._commits.is_committed(seq):
            raise UncommittedRecordError(seq)
        if not self._index.mark(seq):
            raise DuplicateTombstoneError(seq)
        removed = self._journal.stream.remove(seq)
        self._journal.repository.rewrite_records(self._journal.stream.records())
        self._commits.retreat_to(self._journal.stream.head())
        return removed
