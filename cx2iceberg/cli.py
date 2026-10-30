"""Linha de comandos: cx2iceberg {run,validate,ddl,init,transforms}."""
from __future__ import annotations

import argparse
import json
import logging
import sys

from . import config as C
from .engine import Engine, TableResult, TransformError
from .reader import read_documents
from .transforms import REGISTRY
from .writer import AthenaIcebergWriter, ddl, write_local


def _preview(results: dict[str, TableResult], n: int):
    for name, res in results.items():
        print(f"\n=== {name}  ({len(res.rows)} linha(s), mode={res.spec.mode}, keys={res.spec.keys or '-'}"
              f"{', ignoradas=' + str(res.skipped) if res.skipped else ''}"
              f"{', checks falhados=' + str(res.check_failures) if res.check_failures else ''})")
        print("    " + ", ".join(f"{c}:{t}" for c, t in res.types.items()))
        for r in res.rows[:n]:
            print("    " + json.dumps(r, ensure_ascii=False, default=str))


def _transform(args) -> tuple[C.Config, dict[str, TableResult]]:
    cfg = C.load(args.config)
    docs = list(read_documents(args.inputs, cfg.input, semantic_id=args.semantic_id))
    if not docs:
        sys.exit("Nenhum documento encontrado nos inputs")
    results = Engine(cfg, match_all=args.match_all or None).run(docs)
    if args.table:
        results = {k: v for k, v in results.items() if k in args.table}
    return cfg, results


def cmd_run(args):
    cfg, results = _transform(args)
    if args.out:
        for p in write_local(results, args.out, args.format):
            print(f"escrito {p}")
    if args.dry_run or args.out:
        _preview(results, args.show)
        return
    written = AthenaIcebergWriter(cfg.target).write(results)
    for t, n in written.items():
        print(f"{cfg.target['database']}.{t}: {n} linha(s)")


def cmd_ddl(args):
    cfg, results = _transform(args)
    for res in results.values():
        print(ddl(res, cfg.target) + "\n")


def cmd_validate(args):
    cfg = C.load(args.config)
    for s in cfg.submodels:
        print(f"submodelo '{s.name}': match={s.match or '(todos)'}")
        for t in s.tables:
            print(f"  - {t.name}: {len(t.columns)} coluna(s), explode={t.explode or '-'}, mode={t.mode}, keys={t.keys or '-'}")
        if s.auto_normalize is not None:
            print(f"  - auto_normalize: {s.auto_normalize or '(por omissão)'}")
    print("Configuração válida.")


def cmd_init(args):
    from .samm import load_skeleton
    from .scaffold import scaffold

    if not args.inputs and not args.schema:
        sys.exit("Indique JSONs de exemplo e/ou --schema <Aspect-schema.json>")
    docs = [d.data for d in read_documents(args.inputs, {"list_is_document": False})] if args.inputs else []
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
        print(f"Configuração escrita em {args.output}")
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
    p = argparse.ArgumentParser(prog="cx2iceberg", description="Catena-X submodel JSON -> tabelas Iceberg no Athena")
    p.add_argument("-v", "--verbose", action="count", default=0)
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("-c", "--config", required=True)
        sp.add_argument("inputs", nargs="+", help="ficheiros, pastas ou s3://bucket/prefixo")
        sp.add_argument("--semantic-id", help="semantic id (URN) a aplicar a todos os documentos")
        sp.add_argument("--match-all", action="store_true", help="um documento pode alimentar vários submodelos")
        sp.add_argument("--table", action="append", help="processar só esta(s) tabela(s)")

    r = sub.add_parser("run", help="transformar e escrever no Athena")
    common(r)
    r.add_argument("--dry-run", action="store_true", help="não escreve no Athena; mostra pré-visualização")
    r.add_argument("--out", help="escreve as tabelas localmente nesta pasta (implica dry-run)")
    r.add_argument("--format", choices=["csv", "parquet", "jsonl"], default="csv")
    r.add_argument("--show", type=int, default=5, help="linhas a mostrar por tabela")
    r.set_defaults(fn=cmd_run)

    d = sub.add_parser("ddl", help="mostra o CREATE TABLE Iceberg de cada tabela")
    common(d)
    d.set_defaults(fn=cmd_ddl)

    v = sub.add_parser("validate", help="valida a configuração")
    v.add_argument("-c", "--config", required=True)
    v.set_defaults(fn=cmd_validate)

    i = sub.add_parser("init", help="gera uma configuração inicial a partir de JSONs de exemplo e/ou do schema SAMM")
    i.add_argument("inputs", nargs="*")
    i.add_argument("--schema", help="JSON Schema gerado pelo SAMM (…/gen/<Aspect>-schema.json): inclui todos os campos")
    i.add_argument("--name", default="submodel", help="prefixo das tabelas")
    i.add_argument("--semantic-id")
    i.add_argument("--id-path", help="caminho do id do documento (omissão: catenaXId, id ou *.id)")
    i.add_argument("--unwrap", help="'all' ou lista separada por vírgulas de listas 1:1 a tratar como objecto")
    i.add_argument("--keep-list", action="append", help="com --unwrap all: lista que tem mesmo vários elementos (repetível)")
    i.add_argument("--naming", choices=["full", "leaf"], default="full", help="leaf = nomes curtos de coluna")
    i.add_argument("--style", choices=["flat", "struct"], default="flat",
                   help="flat = colunas + tabelas filhas; struct = 1 tabela com colunas struct por bloco")
    i.add_argument("--scalar-lists", choices=["array", "rows"], default="array",
                   help="listas de valores simples: coluna array<...> ou tabela filha (1 linha por valor)")
    i.add_argument("-o", "--output")
    i.set_defaults(fn=cmd_init)

    s = sub.add_parser("studio", help="abre a interface web para ver, editar e pré-visualizar configurações")
    s.add_argument("--root", default=".", help="pasta com as configurações e os dados de exemplo (omissão: a actual)")
    s.add_argument("--port", type=int, default=8765)
    s.add_argument("--open", action="store_true", help="abre o browser")
    s.add_argument("--models-dir", help="clone local de eclipse-tractusx/sldt-semantic-models (sem acesso ao GitHub)")
    s.set_defaults(fn=cmd_studio)

    t = sub.add_parser("transforms", help="lista as transformações disponíveis")
    t.set_defaults(fn=cmd_transforms)

    args = p.parse_args(argv)
    logging.basicConfig(level=[logging.WARNING, logging.INFO, logging.DEBUG][min(args.verbose, 2)],
                        format="%(levelname)s %(message)s")
    try:
        args.fn(args)
    except C.ConfigError as e:
        sys.exit(f"Erro de configuração: {e}")
    except TransformError as e:
        sys.exit(f"Erro de transformação: {e}")


if __name__ == "__main__":
    main()
