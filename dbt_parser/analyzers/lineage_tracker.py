"""Rastreador de linhagem de dados para projetos dbt."""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from dbt_parser.analyzers.graph_resolver import GraphResolver, NodeInfo

logger = logging.getLogger(__name__)


@dataclass
class LineageEntry:
    """Entrada de linhagem de dados."""

    source_node: str
    target_node: str
    lineage_type: str  # "ref", "source", "macro"
    columns: List[str] = field(default_factory=list)
    transformation: str = ""


class LineageTracker:
    """Rastreia linhagem de dados atraves do projeto dbt."""

    def __init__(self, graph_resolver: GraphResolver) -> None:
        self.graph_resolver = graph_resolver
        self._lineage_entries: List[LineageEntry] = []
        self._column_lineage: Dict[str, Dict[str, List[str]]] = {}

    def track_ref_lineage(
        self, model_name: str, referenced_model: str, columns: Optional[List[str]] = None
    ) -> LineageEntry:
        """Registra linhagem via ref()."""
        entry = LineageEntry(
            source_node=referenced_model,
            target_node=model_name,
            lineage_type="ref",
            columns=columns or [],
        )
        self._lineage_entries.append(entry)
        logger.info("Linhagem ref: %s -> %s", referenced_model, model_name)
        return entry

    def track_source_lineage(
        self,
        model_name: str,
        source_name: str,
        table_name: str,
        columns: Optional[List[str]] = None,
    ) -> LineageEntry:
        """Registra linhagem via source()."""
        source_key = f"{source_name}.{table_name}"
        entry = LineageEntry(
            source_node=source_key,
            target_node=model_name,
            lineage_type="source",
            columns=columns or [],
        )
        self._lineage_entries.append(entry)
        logger.info("Linhagem source: %s -> %s", source_key, model_name)
        return entry

    def get_full_lineage(self, model_name: str) -> Dict[str, Any]:
        """Retorna linhagem completa de um modelo (upstream e downstream)."""
        upstream = self.graph_resolver.get_all_upstream(model_name)
        downstream = self.graph_resolver.get_all_downstream(model_name)

        direct_deps = self.graph_resolver.get_dependencies(model_name)
        direct_dependents = self.graph_resolver.get_dependents(model_name)

        return {
            "model": model_name,
            "upstream": sorted(upstream),
            "downstream": sorted(downstream),
            "direct_dependencies": sorted(direct_deps),
            "direct_dependents": sorted(direct_dependents),
            "lineage_depth_up": self._calculate_depth(model_name, direction="up"),
            "lineage_depth_down": self._calculate_depth(model_name, direction="down"),
        }

    def get_lineage_entries(
        self, model_name: Optional[str] = None
    ) -> List[LineageEntry]:
        """Retorna entradas de linhagem, opcionalmente filtradas por modelo."""
        if model_name is None:
            return self._lineage_entries.copy()
        return [
            e
            for e in self._lineage_entries
            if e.source_node == model_name or e.target_node == model_name
        ]

    def get_data_flow_path(self, source: str, target: str) -> List[List[str]]:
        """Encontra caminhos de fluxo de dados entre dois nos."""
        import networkx as nx

        try:
            paths = list(
                nx.all_simple_paths(self.graph_resolver.graph, target, source)
            )
            return paths
        except nx.NetworkXError:
            logger.warning("Caminho nao encontrado: %s -> %s", source, target)
            return []

    def track_column_lineage(
        self, model_name: str, column_name: str, source_columns: List[str]
    ) -> None:
        """Registra linhagem em nivel de coluna."""
        if model_name not in self._column_lineage:
            self._column_lineage[model_name] = {}
        self._column_lineage[model_name][column_name] = source_columns

    def get_column_lineage(self, model_name: str) -> Dict[str, List[str]]:
        """Retorna linhagem de colunas de um modelo."""
        return self._column_lineage.get(model_name, {})

    def _calculate_depth(self, node_name: str, direction: str = "up") -> int:
        """Calcula profundidade maxima de linhagem."""
        import networkx as nx

        if node_name not in self.graph_resolver.graph:
            return 0

        if direction == "up":
            lengths = nx.single_source_shortest_path_length(
                self.graph_resolver.graph, node_name
            )
        else:
            reversed_graph = self.graph_resolver.graph.reverse()
            lengths = nx.single_source_shortest_path_length(reversed_graph, node_name)

        return max(lengths.values()) if lengths else 0

    def get_lineage_summary(self) -> Dict[str, Any]:
        """Retorna resumo da linhagem do projeto."""
        return {
            "total_entries": len(self._lineage_entries),
            "ref_entries": sum(1 for e in self._lineage_entries if e.lineage_type == "ref"),
            "source_entries": sum(1 for e in self._lineage_entries if e.lineage_type == "source"),
            "models_with_column_lineage": len(self._column_lineage),
        }
