"""cx2iceberg — transforma submodelos Catena-X (JSON) em tabelas Iceberg no Athena.

Uso programático:

    from cx2iceberg import load_config, transform, write_athena
    cfg = load_config("config.yaml")
    results = transform(cfg, ["data/serial_part.json"])
    write_athena(cfg, results)
"""
from __future__ import annotations

from .config import Config, ConfigError, from_dict, load as load_config
from .engine import Document, DocumentsRejected, Engine, TableResult, TransformError
from .reader import read_documents
from .report import Report
from .transforms import register

__all__ = ["Config", "ConfigError", "Document", "DocumentsRejected", "Engine", "Report", "TableResult",
           "TransformError", "from_dict", "load_config", "read_documents", "register", "transform", "write_athena"]


def transform(cfg: Config, sources: list[str], semantic_id: str | None = None) -> dict[str, TableResult]:
    """Lança DocumentsRejected se algum documento tiver erros (com .report e .results).

    Para escrever só os documentos válidos: Engine(cfg).run(docs, partial=True) e consultar engine.report.
    """
    docs = list(read_documents(sources, cfg.input, semantic_id=semantic_id))
    return Engine(cfg).run(docs)


def write_athena(cfg: Config, results: dict[str, TableResult], report: Report | None = None) -> dict[str, int]:
    """Com `report`, o relatório fica com o resultado da escrita de cada tabela (escrita, falhou, não escrita)."""
    from .writer import AthenaIcebergWriter

    return AthenaIcebergWriter(cfg.target, report=report).write(results)
