# dbt-date-harvester

Ferramenta de parsing e analise estatica para projetos dbt (data build tool).

## Sobre

O dbt-date-harvester analisa projetos dbt extraindo metadados de arquivos YAML
e SQL, construindo grafos de dependencia e fornecendo ferramentas completas de
analise, validacao e exportacao.

## Funcionalidades

- Parsing de YAML (schema.yml, sources.yml, dbt_project.yml)
- Parsing de SQL com suporte a Jinja (refs, sources, configs, CTEs)
- Construcao de grafos de dependencia com networkx
- Rastreamento de linhagem de dados
- Analise de impacto de mudancas
- Deteccao de modelos nao utilizados
- Metricas de complexidade SQL
- Deteccao de duplicados
- Validacao de nomenclatura (stg_, fct_, dim_)
- Validacao de configuracoes
- Exportacao para JSON, GraphViz DOT e Mermaid
- Busca fuzzy de modelos
- Sistema de filtragem avancado
- Cache de resultados
- Sistema de plugins extensivel
- Interface CLI completa

## Instalacao

```bash
pip install -e .
```

### Dependencias

- Python 3.8+
- pyyaml >= 6.0
- networkx >= 2.8

### Desenvolvimento

```bash
pip install -r requirements-dev.txt
make test
```

## Uso

### CLI

```bash
# Parsing
dbt-parser parse --project-dir /caminho/projeto

# Grafo de dependencias
dbt-parser graph --project-dir /caminho/projeto

# Analise de modelo especifico
dbt-parser graph --project-dir /caminho/projeto --model fct_event_dates

# Validacao
dbt-parser validate --project-dir /caminho/projeto --severity warning

# Linhagem de dados
dbt-parser lineage --project-dir /caminho/projeto --model fct_event_dates

# Exportacao
dbt-parser export --project-dir /caminho/projeto --format json --output graph.json
dbt-parser export --project-dir /caminho/projeto --format dot --output graph.dot
dbt-parser export --project-dir /caminho/projeto --format mermaid --output graph.mmd
```

### Como Biblioteca

```python
from pathlib import Path
from dbt_parser.parsers import YamlParser, SqlParser, SchemaExtractor
from dbt_parser.analyzers import GraphResolver, DependencyAnalyzer
from dbt_parser.exporters import JsonExporter

project = Path("/caminho/projeto/dbt")

yaml_parser = YamlParser(project)
sql_parser = SqlParser(project)
yaml_parser.parse_all()
sql_parser.parse_all()

graph = GraphResolver()
analyzer = DependencyAnalyzer(graph, sql_parser)
analyzer.build_dependency_graph()

exporter = JsonExporter()
exporter.export_to_file(
    exporter.export_graph(graph),
    Path("output/graph.json")
)
```

## Estrutura do Projeto

```
dbt_parser/
  cli.py            - Interface de linha de comando
  parsers/          - Parsing de YAML, SQL e Jinja
  analyzers/        - Grafos, linhagem, dependencias, impacto
  validators/       - Validacao de modelos, nomes, configs
  exporters/        - Exportacao JSON, GraphViz, Mermaid
  utils/            - Cache, busca fuzzy, performance
  plugins/          - Sistema de plugins extensivel
tests/              - Suite completa de testes pytest
docs/               - Documentacao detalhada
```

## Testes

```bash
# Executar todos os testes
make test

# Com cobertura
make test-cov
```

## Documentacao

- [Arquitetura](docs/architecture.md)
- [Features](docs/features.md)
- [Exemplos](docs/examples.md)
- [Tutorial](docs/tutorial.md)
- [API Reference](docs/api_reference.md)

## Licenca

Uso interno.
