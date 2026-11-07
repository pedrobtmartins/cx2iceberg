"""Notas das conversões e definições da carga, sem as passar como argumento por todas as funções.

* `note(...)`: uma transformação ou um cast regista uma perda declarada ou um aviso (ex.: valor arredondado
  com `rounding` declarado, nulos ignorados com `sum: {nulls: skip}`, unidade de fallback). O motor recolhe as
  notas de cada valor e põe-nas no relatório, com tabela, coluna, ficheiro e linha. Fora do motor são ignoradas.
* `setting(...)`: definições da carga (ex.: `input.naive_timestamps`), visíveis nas conversões.

Usa contextvars: cada carga (e cada thread) tem as suas.
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
    """Activa a recolha de notas (e as definições) durante uma carga. Devolve a lista de notas."""
    notes: list = []
    t1, t2 = _NOTES.set(notes), _SETTINGS.set(dict(settings or {}))
    try:
        yield notes
    finally:
        _NOTES.reset(t1)
        _SETTINGS.reset(t2)
