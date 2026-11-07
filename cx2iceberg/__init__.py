"""cx2iceberg — turns Catena-X submodels (JSON) into Iceberg tables in Athena.

Programmatic use:

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
    """Raises DocumentsRejected if any document has errors (with .report and .results).

    To write only the valid documents: Engine(cfg).run(docs, partial=True) and look at engine.report.
    """
    docs = list(read_documents(sources, cfg.input, semantic_id=semantic_id))
    return Engine(cfg).run(docs)


def write_athena(cfg: Config, results: dict[str, TableResult], report: Report | None = None) -> dict[str, int]:
    """With `report`, the report gets the write result of each table (written, failed, not written)."""
    from .writer import AthenaIcebergWriter

    return AthenaIcebergWriter(cfg.target, report=report).write(results)
