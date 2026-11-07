"""AWS Lambda handler for S3 events (e.g. files delivered by the EDC into a bucket).

Environment variables:
    CX2ICEBERG_CONFIG   local path (in the package) or s3://bucket/config.yaml

All or nothing per invocation: if any document of the event is rejected, nothing is written and the invocation
fails (the report goes to the log). That way a retry does not duplicate rows already written. Configure
MaximumRetryAttempts=0 and an on-failure destination (or DLQ): retrying does not fix errors in the data and, without
a destination, AWS discards the event after the attempts.

After the write nothing can fail: the response (and the log text) is built and validated before writing.
A failure after the write would mark the invocation as failed with the data already written; AWS would retry
(duplicate rows in `append` tables) or send the event to the on-failure destination, and reprocessing it
would duplicate the rows again.
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

# The response carries only counts (no examples): it must fit within AWS limits (6 MB synchronous, 256 KB for
# asynchronous destinations). The full report goes to the log.
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
        results = engine.run(list(read_documents(sources, cfg.input)))   # a rejected document -> exception
    except TransformError:
        log.error("%s", report.render())
        raise
    # everything that can fail happens before the write
    summary = report.to_dict(examples=0, max_rejected=MAX_REJECTED_IN_RESPONSE)
    json.dumps(summary, allow_nan=False)          # fails here (nothing written) if any value is not strict JSON
    try:
        written = write_athena(cfg, results, report=report)   # dict[str, int]
    except Exception:
        report.finish_write()
        log.error("%s", report.render())          # what was written and what failed
        raise
    summary["write"] = report.write                # only text and integers (built by the report)
    summary["status"] = report.status
    # the default log level in Lambda is WARNING: with declared losses or warnings, the report must reach the
    # log (in an asynchronous invocation the response is discarded, unless a destination is configured)
    try:                                           # after the write nothing can fail (see docstring)
        text = report.render()
    except Exception:  # noqa: BLE001
        text = f"report unavailable (status={report.status})"
    log.log(logging.WARNING if report.groups else logging.INFO, "%s", text)
    return {"status": report.status, "written": written, "report": summary}
