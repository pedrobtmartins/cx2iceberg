"""Notes from the conversions and the load's settings, without passing them as an argument through every function.

* `note(...)`: a transformation or a cast records a declared loss or a warning (e.g. a value rounded
  with `rounding` declared, nulls skipped with `sum: {nulls: skip}`, a fallback unit). The engine collects the
  notes of each value and puts them in the report, with table, column, file and row. Outside the engine they are ignored.
* `setting(...)`: the load's settings (e.g. `input.naive_timestamps`), visible in the conversions.

Uses contextvars: each load (and each thread) has its own.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

_NOTES: ContextVar[list | None] = ContextVar("cx2iceberg_notes", default=None)
_SETTINGS: ContextVar[dict] = ContextVar("cx2iceberg_settings", default={})


def note(kind: str, reason: str, action: str, message: str) -> None:
    notes = _NOTES.get()
    if notes is not None:
        notes.append((kind, reason, action, message))


def setting(name: str, default: Any = None) -> Any:
    return _SETTINGS.get().get(name, default)


@contextmanager
def scope(settings: dict | None = None) -> Iterator[list]:
    """Turns on the collection of notes (and the settings) during a load. Returns the list of notes."""
    notes: list = []
    t1, t2 = _NOTES.set(notes), _SETTINGS.set(dict(settings or {}))
    try:
        yield notes
    finally:
        _NOTES.reset(t1)
        _SETTINGS.reset(t2)
