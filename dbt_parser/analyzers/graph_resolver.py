"""Resolver de grafos de dependencia para projetos dbt."""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx

logger = logging.getLogger(__name__)


@dataclass
class NodeInfo:
    """Informacoes de um no no grafo de dependencias."""

    name: str
    node_type: str  # "model", "source", "seed", "test"
    filepath: Optional[str] = None
    config: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)


class GraphResolver:
    """Constroi e resolve grafos de dependencia de projetos dbt."""

    def __init__(self) -> None:
        self.graph: nx.DiGraph = nx.DiGraph()
        self._nodes: Dict[str, NodeInfo] = {}

    def add_node(self, node: NodeInfo) -> None:
        """Adiciona um no ao grafo."""
        self.graph.add_node(node.name, **{
            "node_type": node.node_type,
            "filepath": node.filepath,
            "config": node.config,
            "tags": node.tags,
        })
        self._nodes[node.name] = node
        logger.debug("No adicionado: %s (%s)", node.name, node.node_type)

    def add_edge(self, from_node: str, to_node: str) -> None:
        """Adiciona aresta de dependencia (from depende de to)."""
        self.graph.add_edge(from_node, to_node)
        logger.debug("Aresta adicionada: %s -> %s", from_node, to_node)

    def get_dependencies(self, node_name: str) -> Set[str]:
        """Retorna dependencias diretas de um no."""
        if node_name not in self.graph:
            return set()
        return set(self.graph.successors(node_name))

    def get_dependents(self, node_name: str) -> Set[str]:
        """Retorna nos que dependem diretamente deste no."""
        if node_name not in self.graph:
            return set()
        return set(self.graph.predecessors(node_name))

    def get_all_upstream(self, node_name: str) -> Set[str]:
        """Retorna todas as dependencias transitivas (upstream)."""
        if node_name not in self.graph:
            return set()
        return set(nx.descendants(self.graph, node_name))

    def get_all_downstream(self, node_name: str) -> Set[str]:
        """Retorna todos os dependentes transitivos (downstream)."""
        if node_name not in self.graph:
            return set()
        return set(nx.ancestors(self.graph, node_name))

    def topological_sort(self) -> List[str]:
        """Retorna ordem topologica de execucao."""
        try:
            return list(reversed(list(nx.topological_sort(self.graph))))
        except nx.NetworkXUnfeasible:
            logger.error("Ciclo detectado no grafo de dependencias")
            raise ValueError("Ciclo detectado no grafo de dependencias")

    def detect_cycles(self) -> List[List[str]]:
        """Detecta ciclos no grafo."""
        try:
            cycles = list(nx.simple_cycles(self.graph))
            if cycles:
                logger.warning("Ciclos detectados: %d", len(cycles))
            return cycles
        except nx.NetworkXError:
            return []

    def get_root_nodes(self) -> Set[str]:
        """Retorna nos raiz (sem dependencias)."""
        return {n for n in self.graph.nodes() if self.graph.out_degree(n) == 0}

    def get_leaf_nodes(self) -> Set[str]:
        """Retorna nos folha (sem dependentes)."""
        return {n for n in self.graph.nodes() if self.graph.in_degree(n) == 0}

    def get_node_info(self, name: str) -> Optional[NodeInfo]:
        """Retorna informacoes de um no."""
        return self._nodes.get(name)

    def node_count(self) -> int:
        """Retorna numero de nos no grafo."""
        return self.graph.number_of_nodes()

    def edge_count(self) -> int:
        """Retorna numero de arestas no grafo."""
        return self.graph.number_of_edges()

    def get_subgraph(self, nodes: Set[str]) -> "GraphResolver":
        """Retorna subgrafo contendo apenas os nos especificados."""
        sub = GraphResolver()
        sub.graph = self.graph.subgraph(nodes).copy()
        for name in nodes:
            if name in self._nodes:
                sub._nodes[name] = self._nodes[name]
        return sub
