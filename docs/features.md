# Guia de Features - dbt-date-harvester

## Parsing

### YAML Parser
Faz parsing de todos os arquivos YAML do projeto dbt.

```python
from dbt_parser.parsers.yaml_parser import YamlParser

parser = YamlParser(project_dir)
results = parser.parse_all()
```

### SQL Parser
Extrai refs, sources, configs e CTEs de arquivos SQL.

```python
from dbt_parser.parsers.sql_parser import SqlParser

parser = SqlParser(project_dir)
models = parser.parse_all()

for name, model in models.items():
    print(f"{name}: refs={model.refs}, sources={model.sources}")
```

### Jinja Parser
Analisa templates Jinja em SQL dbt.

```python
from dbt_parser.parsers.jinja_parser import JinjaParser

parser = JinjaParser()
analysis = parser.parse_content(sql_content, "model.sql")
print(f"Variaveis: {analysis.variables}")
```

## Analise

### Grafo de Dependencias
Constroi e consulta grafo de dependencias entre modelos.

```python
from dbt_parser.analyzers.graph_resolver import GraphResolver
from dbt_parser.analyzers.dependency_analyzer import DependencyAnalyzer

graph = GraphResolver()
analyzer = DependencyAnalyzer(graph, sql_parser)
analyzer.build_dependency_graph()
order = analyzer.get_execution_order()
```

### Linhagem de Dados
Rastreia linhagem completa de dados.

```python
from dbt_parser.analyzers.lineage_tracker import LineageTracker

tracker = LineageTracker(graph)
lineage = tracker.get_full_lineage("fct_event_dates")
```

### Analise de Impacto
Avalia impacto de mudancas em modelos.

```python
from dbt_parser.analyzers.impact_analyzer import ImpactAnalyzer

impact = ImpactAnalyzer(graph)
report = impact.analyze_impact("stg_events")
print(f"Risco: {report.risk_level}")
```

### Detector de Nao Utilizados
Encontra modelos, sources e macros sem uso.

```python
from dbt_parser.analyzers.unused_detector import UnusedDetector

detector = UnusedDetector(graph)
report = detector.generate_report()
```

### Metricas de Complexidade
Calcula score de complexidade por modelo.

```python
from dbt_parser.analyzers.complexity_metrics import ComplexityMetrics

metrics = ComplexityMetrics(sql_parser, graph)
top_complex = metrics.get_most_complex(5)
```

## Validacao

### Model Validator
Valida modelos contra boas praticas dbt.

### Naming Validator
Valida convencoes de nomenclatura (stg_, fct_, dim_).

### Config Validator
Valida dbt_project.yml e configs de modelos.

## Exportacao

### JSON
Exporta grafos e relatorios para JSON.

### GraphViz DOT
Gera diagramas de dependencia em formato DOT.

### Mermaid
Gera diagramas Mermaid para documentacao.

## Busca

### Busca Fuzzy
Encontra modelos por nome com tolerancia a erros de digitacao.

```python
from dbt_parser.utils.search import FuzzySearch

search = FuzzySearch(graph)
results = search.search("stg_evnts")  # encontra stg_events
```

## Cache
Cache em memoria para resultados de parsing com TTL configuravel.
