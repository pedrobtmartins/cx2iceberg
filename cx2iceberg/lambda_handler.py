"""Handler AWS Lambda para eventos S3 (ex.: ficheiros entregues pelo EDC num bucket).

Variáveis de ambiente:
    CX2ICEBERG_CONFIG   caminho local (no pacote) ou s3://bucket/config.yaml
"""
from __future__ import annotations

import os
import tempfile
import urllib.parse

from . import load_config, transform, write_athena


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
        return {"written": {}}
    return {"written": write_athena(cfg, transform(cfg, sources))}
