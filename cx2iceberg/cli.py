"""Command line: cx2iceberg {run,validate,ddl,init,transforms}."""
from __future__ import annotations

import argparse
import json
import logging
import sys

from . import config as C
from .engine import Engine, TableResult, TransformError
from .reader import read_documents
from .report import Report
from .transforms import REGISTRY
from .writer import AthenaIcebergWriter, ddl, write_local


def _preview(results: dict[str, TableResult], n: int):
    for name, res in results.items():
        print(f"\n=== {name}  ({len(res.rows)} row(s), mode={res.spec.mode}, keys={res.spec.keys or '-'}"
              f"{', skipped=' + str(res.skipped) if res.skipped else ''}"
              f"{', failed checks=' + str(res.check_failures) if res.check_failures else ''})")
        print("    " + ", ".join(f"{c}:{t}" for c, t in res.types.items()))
        for r in res.rows[:n]:
            print("    " + json.dumps(r, ensure_ascii=False, default=str))


# Exit codes: 0 = everything written; 2 = partial load (documents rejected, the others written);
# 1 = failure (configuration, error limit, write): nothing written by cx2iceberg at this stage.
EXIT_PARTIAL = 2


def _save_report(report: Report, args) -> None:
    """Called only once, at the end (also when the write fails): the report says it all."""
    report.finish_write()
    report.close()
    print(report.render(), file=sys.stderr)
    if getattr(args, "report", None):
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, ensure_ascii=False, indent=2, allow_nan=False)


def _transform(args) -> tuple[C.Config, dict[str, TableResult], Report]:
    cfg = C.load(args.config)
    docs = list(read_documents(args.inputs, cfg.input, semantic_id=args.semantic_id))
    if not docs:
        sys.exit("No documents found in the inputs")
    report = Report(samples=args.samples, max_errors=args.max_errors, details_path=args.report_details)
    try:
        # partial=True: rejected documents are left out and the others go on; the report says which
        results = Engine(cfg, match_all=args.match_all or None, report=report).run(docs, partial=True)
    except (C.ConfigError, TransformError):
        _save_report(report, args)
        raise
    if args.table:
        results = {k: v for k, v in results.items() if k in args.table}
    return cfg, results, report


def _finish(report: Report, written: bool) -> None:
    if report.status == "partial":
        what = "the rows of the others were written" if written else "nothing was written (preview)"
        print(f"Partial load: {len(report.rejected)} document(s) rejected; {what}.", file=sys.stderr)
        sys.exit(EXIT_PARTIAL)


def _precheck_offline(results: dict[str, TableResult], report: Report) -> None:
    """In the dry run, the same write pre-check that does not need AWS (Arrow types, awswrangler, partitions)."""
    from .writer import WriteCheckError, check_offline, check_table

    problems = []
    for res in results.values():
        try:
            check_table(res)
            if res.rows:
                check_offline(res)
        except WriteCheckError as e:
            problems.append(str(e))
    if problems:
        raise WriteCheckError("write pre-check:\n  " + "\n  ".join(problems))


def cmd_run(args):
    cfg, results, report = _transform(args)
    athena = not (args.dry_run or args.out)
    try:
        if args.out:
            for p in write_local(results, args.out, args.format, report=report):
                print(f"written {p}")
        if not athena:
            _precheck_offline(results, report)
            _preview(results, args.show)
        else:
            for t, n in AthenaIcebergWriter(cfg.target, report=report).write(results).items():
                print(f"{cfg.target['database']}.{t}: {n} row(s)")
    except Exception as e:
        if report.failure is None:   # e.g. a failure before the 1st table (connection, database)
            report.failure = f"write: {e}"
        raise SystemExit(f"Write error: {e}") from e
    finally:
        _save_report(report, args)
    _finish(report, written=athena)


def cmd_ddl(args):
    cfg, results, report = _transform(args)
    _save_report(report, args)
    for res in results.values():
        print(ddl(res, cfg.target) + "\n")
    _finish(report, written=False)


def cmd_validate(args):
    cfg = C.load(args.config)
    for s in cfg.submodels:
        print(f"submodel '{s.name}': match={s.match or '(all)'}")
        for t in s.tables:
            print(f"  - {t.name}: {len(t.columns)} column(s), explode={t.explode or '-'}, mode={t.mode}, keys={t.keys or '-'}")
        if s.auto_normalize is not None:
            print(f"  - auto_normalize: {s.auto_normalize or '(default)'}")
    print("Configuration is valid.")


def cmd_init(args):
    from .samm import load_skeleton
    from .scaffold import scaffold

    if not args.inputs and not args.schema:
        sys.exit("Give example JSONs and/or --schema <Aspect-schema.json>")
    read = list(read_documents(args.inputs, {"list_is_document": False})) if args.inputs else []
    bad = [f"{d.source_file}: {d.error}" for d in read if d.error]
    if bad:
        sys.exit("Could not read:\n  " + "\n  ".join(bad))
    docs = [d.data for d in read]
    semantic_id = args.semantic_id
    if args.schema:
        skel, urn = load_skeleton(args.schema)
        docs.insert(0, skel)
        semantic_id = semantic_id or urn
    unwrap_rule = None
    if args.unwrap == "all":
        unwrap_rule = {"all": True, **({"except": args.keep_list} if args.keep_list else {})}
    elif args.unwrap:
        unwrap_rule = [p.strip() for p in args.unwrap.split(",")]
    text = scaffold(docs, args.name, semantic_id, args.id_path, unwrap_rule, args.naming,
                    args.style == "struct", args.scalar_lists)
    if args.output:
        open(args.output, "w", encoding="utf-8").write(text)
        print(f"Configuration written to {args.output}")
    else:
        print(text)


def cmd_studio(args):
    from .studio.server import serve

    serve(args.root, args.port, args.open, args.models_dir)


def cmd_transforms(args):
    for name in sorted(REGISTRY):
        doc = (REGISTRY[name].__doc__ or "").strip().splitlines()
        print(f"{name:20s} {doc[0] if doc else ''}")


def main(argv=None):
    p = argparse.ArgumentParser(prog="cx2iceberg", description="Catena-X submodel JSON -> Iceberg tables in Athena")
    p.add_argument("-v", "--verbose", action="count", default=0)
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("-c", "--config", required=True)
        sp.add_argument("inputs", nargs="+", help="files, folders or s3://bucket/prefix")
        sp.add_argument("--semantic-id", help="semantic id (URN) to apply to every document")
        sp.add_argument("--match-all", action="store_true", help="a document can feed several submodels")
        sp.add_argument("--table", action="append", help="process only this table (or these tables)")
        sp.add_argument("--report", help="writes the report (JSON: counts and examples per table/column/reason)")
        sp.add_argument("--report-details", help="writes every event, one per line (JSON Lines), without keeping them in memory")
        sp.add_argument("--max-errors", type=int, default=1000, help="stops the load after N errors (default: 1000)")
        sp.add_argument("--samples", type=int, default=20, help="examples kept per report group (default: 20)")

    r = sub.add_parser("run", help="transform and write to Athena")
    common(r)
    r.add_argument("--dry-run", action="store_true", help="does not write to Athena; shows a preview")
    r.add_argument("--out", help="writes the tables locally into this folder (implies dry-run)")
    r.add_argument("--format", choices=["csv", "parquet", "jsonl"], default="csv")
    r.add_argument("--show", type=int, default=5, help="rows to show per table")
    r.set_defaults(fn=cmd_run)

    d = sub.add_parser("ddl", help="shows the Iceberg CREATE TABLE of each table")
    common(d)
    d.set_defaults(fn=cmd_ddl)

    v = sub.add_parser("validate", help="validates the configuration")
    v.add_argument("-c", "--config", required=True)
    v.set_defaults(fn=cmd_validate)

    i = sub.add_parser("init", help="generates an initial configuration from example JSONs and/or the SAMM schema")
    i.add_argument("inputs", nargs="*")
    i.add_argument("--schema", help="JSON Schema generated by SAMM (…/gen/<Aspect>-schema.json): includes every field")
    i.add_argument("--name", default="submodel", help="table prefix")
    i.add_argument("--semantic-id")
    i.add_argument("--id-path", help="path of the document id (default: catenaXId, id or *.id)")
    i.add_argument("--unwrap", help="'all' or a comma-separated list of 1:1 lists to treat as objects")
    i.add_argument("--keep-list", action="append", help="with --unwrap all: a list that really has several elements (repeatable)")
    i.add_argument("--naming", choices=["full", "leaf"], default="full", help="leaf = short column names")
    i.add_argument("--style", choices=["flat", "struct"], default="flat",
                   help="flat = columns + child tables; struct = 1 table with struct columns per block")
    i.add_argument("--scalar-lists", choices=["array", "rows"], default="array",
                   help="lists of simple values: an array<...> column or a child table (1 row per value)")
    i.add_argument("-o", "--output")
    i.set_defaults(fn=cmd_init)

    s = sub.add_parser("studio", help="opens the web interface to view, edit and preview configurations")
    s.add_argument("--root", default=".", help="folder with the configurations and the example data (default: the current one)")
    s.add_argument("--port", type=int, default=8765)
    s.add_argument("--open", action="store_true", help="opens the browser")
    s.add_argument("--models-dir", help="local clone of eclipse-tractusx/sldt-semantic-models (without GitHub access)")
    s.set_defaults(fn=cmd_studio)

    t = sub.add_parser("transforms", help="lists the available transformations")
    t.set_defaults(fn=cmd_transforms)

    args = p.parse_args(argv)
    logging.basicConfig(level=[logging.WARNING, logging.INFO, logging.DEBUG][min(args.verbose, 2)],
                        format="%(levelname)s %(message)s")
    try:
        args.fn(args)
    except C.ConfigError as e:
        sys.exit(f"Configuration error: {e}")
    except TransformError as e:
        sys.exit(f"Transformation error: {e}")


if __name__ == "__main__":
    main()
