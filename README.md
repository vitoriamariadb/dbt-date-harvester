# dbt-date-harvester

Ferramenta de parsing e análise estática para projetos dbt (data build tool).

## Sobre

O dbt-date-harvester analisa projetos dbt extraindo metadados de arquivos YAML
e SQL, construindo grafos de dependência e fornecendo ferramentas completas de
análise, validação e exportação.

## Funcionalidades

- Parsing de YAML (schema.yml, sources.yml, dbt_project.yml)
- Parsing de SQL com suporte a Jinja (refs, sources, configs, CTEs)
- Construção de grafos de dependência com networkx
- Rastreamento de linhagem de dados
- Análise de impacto de mudanças
- Detecção de modelos não utilizados
- Métricas de complexidade SQL
- Detecção de duplicados
- Validação de nomenclatura (stg_, fct_, dim_)
- Validação de configurações
- Exportação para JSON, GraphViz DOT e Mermaid
- Busca fuzzy de modelos
- Sistema de filtragem avançado
- Cache de resultados
- Sistema de plugins extensível
- Interface CLI completa

## Instalação

```bash
pip install -e .
```

### Dependências

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

# Grafo de dependências
dbt-parser graph --project-dir /caminho/projeto

# Análise de modelo específico
dbt-parser graph --project-dir /caminho/projeto --model fct_event_dates

# Validação
dbt-parser validate --project-dir /caminho/projeto --severity warning

# Linhagem de dados
dbt-parser lineage --project-dir /caminho/projeto --model fct_event_dates

# Exportação
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
  analyzers/        - Grafos, linhagem, dependências, impacto
  validators/       - Validação de modelos, nomes, configs
  exporters/        - Exportação JSON, GraphViz, Mermaid
  utils/            - Cache, busca fuzzy, performance
  plugins/          - Sistema de plugins extensível
tests/              - Suite completa de testes pytest
docs/               - Documentação detalhada
```

## Testes

```bash
# Executar todos os testes
make test

# Com cobertura
make test-cov
```

## Documentação

- [Arquitetura](docs/architecture.md)
- [Features](docs/features.md)
- [Exemplos](docs/examples.md)
- [Tutorial](docs/tutorial.md)
- [API Reference](docs/api_reference.md)

## Licença

Uso interno.
