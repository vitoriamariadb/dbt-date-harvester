"""Interface de linha de comando para dbt-date-harvester."""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional

from dbt_parser import __version__

logger = logging.getLogger(__name__)


def setup_logging(verbosity: int) -> None:
    """Configura logging baseado no nivel de verbosidade."""
    levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    level = levels.get(verbosity, logging.DEBUG)

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(handler)


def create_parser() -> argparse.ArgumentParser:
    """Cria parser de argumentos da CLI."""
    parser = argparse.ArgumentParser(
        prog="dbt-parser",
        description="Ferramenta de parsing e analise de projetos dbt",
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    parser.add_argument(
        "--project-dir",
        type=Path,
        default=Path("."),
        help="Diretorio raiz do projeto dbt (default: diretorio atual)",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Aumenta verbosidade (use -vv para debug)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Comandos disponiveis")

    parse_cmd = subparsers.add_parser("parse", help="Faz parsing do projeto dbt")
    parse_cmd.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Formato de saida",
    )

    graph_cmd = subparsers.add_parser("graph", help="Analisa grafo de dependencias")
    graph_cmd.add_argument(
        "--model",
        type=str,
        help="Modelo especifico para analise",
    )

    validate_cmd = subparsers.add_parser("validate", help="Valida projeto dbt")
    validate_cmd.add_argument(
        "--severity",
        choices=["error", "warning", "info"],
        default="warning",
        help="Severidade minima para exibicao",
    )

    lineage_cmd = subparsers.add_parser("lineage", help="Mostra linhagem de dados")
    lineage_cmd.add_argument(
        "--model",
        type=str,
        required=True,
        help="Modelo para rastrear linhagem",
    )

    return parser


def run_parse(args: argparse.Namespace) -> int:
    """Executa comando de parsing."""
    from dbt_parser.parsers.yaml_parser import YamlParser
    from dbt_parser.parsers.sql_parser import SqlParser
    from dbt_parser.parsers.schema_extractor import SchemaExtractor

    project_dir = args.project_dir.resolve()
    if not project_dir.exists():
        logger.error("Diretorio nao encontrado: %s", project_dir)
        return 1

    yaml_parser = YamlParser(project_dir)
    sql_parser = SqlParser(project_dir)

    yaml_files = yaml_parser.parse_all()
    sql_models = sql_parser.parse_all()

    print(f"Projeto: {project_dir}")
    print(f"Arquivos YAML: {len(yaml_files)}")
    print(f"Modelos SQL: {len(sql_models)}")

    for name, model in sql_models.items():
        print(f"  - {name}: refs={model.refs}, sources={model.sources}")

    return 0


def run_graph(args: argparse.Namespace) -> int:
    """Executa comando de analise de grafo."""
    from dbt_parser.parsers.sql_parser import SqlParser
    from dbt_parser.analyzers.graph_resolver import GraphResolver
    from dbt_parser.analyzers.dependency_analyzer import DependencyAnalyzer

    project_dir = args.project_dir.resolve()
    sql_parser = SqlParser(project_dir)
    sql_parser.parse_all()

    graph = GraphResolver()
    analyzer = DependencyAnalyzer(graph, sql_parser)
    analyzer.build_dependency_graph()

    if args.model:
        report = analyzer.analyze_model(args.model)
        print(f"Modelo: {report.model_name}")
        print(f"  Dependencias diretas: {report.direct_dependencies}")
        print(f"  Dependentes diretos: {report.direct_dependents}")
    else:
        print(f"Nos: {graph.node_count()}")
        print(f"Arestas: {graph.edge_count()}")
        print(f"Raizes: {sorted(graph.get_root_nodes())}")
        print(f"Folhas: {sorted(graph.get_leaf_nodes())}")

    return 0


def run_validate(args: argparse.Namespace) -> int:
    """Executa comando de validacao."""
    from dbt_parser.parsers.yaml_parser import YamlParser
    from dbt_parser.parsers.sql_parser import SqlParser
    from dbt_parser.parsers.schema_extractor import SchemaExtractor
    from dbt_parser.validators.model_validator import ModelValidator, Severity

    project_dir = args.project_dir.resolve()
    yaml_parser = YamlParser(project_dir)
    sql_parser = SqlParser(project_dir)
    extractor = SchemaExtractor(yaml_parser)

    yaml_parser.parse_all()
    sql_parser.parse_all()

    for content in yaml_parser.get_parsed_files().values():
        if "models" in content:
            extractor.extract_models(content)

    validator = ModelValidator(extractor, sql_parser)
    results = validator.validate_all()

    severity_map = {"error": Severity.ERROR, "warning": Severity.WARNING, "info": Severity.INFO}
    min_severity = severity_map[args.severity]

    severity_order = [Severity.ERROR, Severity.WARNING, Severity.INFO]
    max_idx = severity_order.index(min_severity)
    allowed = set(severity_order[: max_idx + 1])

    filtered = [r for r in results if r.severity in allowed]
    for result in filtered:
        print(f"[{result.severity.value.upper()}] {result.model_name}: {result.message}")

    summary = validator.get_summary()
    print(f"\nResumo: {summary['errors']} erros, {summary['warnings']} avisos, {summary['info']} info")

    return 1 if summary["errors"] > 0 else 0


def run_lineage(args: argparse.Namespace) -> int:
    """Executa comando de linhagem."""
    from dbt_parser.parsers.sql_parser import SqlParser
    from dbt_parser.analyzers.graph_resolver import GraphResolver
    from dbt_parser.analyzers.dependency_analyzer import DependencyAnalyzer
    from dbt_parser.analyzers.lineage_tracker import LineageTracker

    project_dir = args.project_dir.resolve()
    sql_parser = SqlParser(project_dir)
    sql_parser.parse_all()

    graph = GraphResolver()
    dep_analyzer = DependencyAnalyzer(graph, sql_parser)
    dep_analyzer.build_dependency_graph()

    tracker = LineageTracker(graph)
    lineage = tracker.get_full_lineage(args.model)

    print(f"Linhagem de: {lineage['model']}")
    print(f"  Upstream: {lineage['upstream']}")
    print(f"  Downstream: {lineage['downstream']}")
    print(f"  Profundidade upstream: {lineage['lineage_depth_up']}")
    print(f"  Profundidade downstream: {lineage['lineage_depth_down']}")

    return 0


COMMAND_MAP = {
    "parse": run_parse,
    "graph": run_graph,
    "validate": run_validate,
    "lineage": run_lineage,
}


def main(argv: Optional[List[str]] = None) -> int:
    """Ponto de entrada principal da CLI."""
    parser = create_parser()
    args = parser.parse_args(argv)

    setup_logging(args.verbose)

    if not args.command:
        parser.print_help()
        return 0

    command_func = COMMAND_MAP.get(args.command)
    if command_func:
        return command_func(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
