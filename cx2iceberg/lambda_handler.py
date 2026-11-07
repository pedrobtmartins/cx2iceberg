"""Handler AWS Lambda para eventos S3 (ex.: ficheiros entregues pelo EDC num bucket).

Variáveis de ambiente:
    CX2ICEBERG_CONFIG   caminho local (no pacote) ou s3://bucket/config.yaml

Tudo ou nada por invocação: se algum documento do evento for rejeitado, nada é escrito e a invocação
falha (o relatório vai para o log). Assim uma nova tentativa não duplica linhas já escritas. Configure
MaximumRetryAttempts=0 e um destino on-failure (ou DLQ): repetir não corrige erros nos dados e, sem destino,
a AWS descarta o evento depois das tentativas.

Depois da escrita nada pode falhar: a resposta (e o texto do log) é construída e validada antes de escrever.
Uma falha depois da escrita marcaria a invocação como falhada com os dados já escritos; a AWS repetiria
(linhas duplicadas nas tabelas `append`) ou mandaria o evento para o destino on-failure, e reprocessá-lo
duplicaria as linhas outra vez.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import urllib.parse

from . import load_config, write_athena
from .engine import Engine, TransformError
from .reader import read_documents
from .report import Report

log = logging.getLogger("cx2iceberg")

# A resposta leva só contagens (sem exemplos): tem de caber nos limites da AWS (6 MB síncrono, 256 KB nos destinos
# assíncronos). O relatório completo vai para o log.
MAX_REJECTED_IN_RESPONSE = 100


def _config_path() -> str:
    path = os.environ["CX2ICEBERG_CONFIG"]
    if not path.startswith("s3://"):
        return path
    import boto3

    bucket, _, key = path[5:].partition("/")
    local = os.path.join(tempfile.gettempdir(), "cx2iceberg.yaml")
    boto3.client("s3").download_file(bucket, key, local)
    return local


def handler(event, context):
    cfg = load_config(_config_path())
    sources = [
        f"s3://{r['s3']['bucket']['name']}/{urllib.parse.unquote_plus(r['s3']['object']['key'])}"
        for r in event.get("Records", [])
        if "s3" in r
    ]
    if not sources:
        return {"status": "ok", "written": {}, "report": None}
    report = Report()
    engine = Engine(cfg, report=report)
    try:
        results = engine.run(list(read_documents(sources, cfg.input)))   # documento rejeitado -> excepção
    except TransformError:
        log.error("%s", report.render())
        raise
    # tudo o que pode falhar acontece antes da escrita
    summary = report.to_dict(examples=0, max_rejected=MAX_REJECTED_IN_RESPONSE)
    json.dumps(summary, allow_nan=False)          # falha aqui (nada escrito) se algum valor não for JSON estrito
    try:
        written = write_athena(cfg, results, report=report)   # dict[str, int]
    except Exception:
        report.finish_write()
        log.error("%s", report.render())          # o que foi escrito e o que falhou
        raise
    summary["write"] = report.write                # só texto e inteiros (construído pelo relatório)
    summary["status"] = report.status
    # o nível por omissão dos logs na Lambda é WARNING: com perdas declaradas ou avisos, o relatório tem de lá
    # chegar (numa invocação assíncrona a resposta é descartada, a não ser com um destino configurado)
    try:                                           # depois da escrita nada pode falhar (ver docstring)
        text = report.render()
    except Exception:  # noqa: BLE001
        text = f"relatório indisponível (estado={report.status})"
    log.log(logging.WARNING if report.groups else logging.INFO, "%s", text)
    return {"status": report.status, "written": written, "report": summary}
