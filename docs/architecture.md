# Arquitetura - dbt-date-harvester

## Visao Geral

O dbt-date-harvester e uma ferramenta de analise estatica para projetos dbt.
Faz parsing de arquivos YAML e SQL, constroi grafos de dependencia e fornece
ferramentas de validacao e analise.

## Componentes Principais

### Parsers

- **YamlParser**: Parsing de arquivos YAML (schema.yml, sources.yml, dbt_project.yml)
- **SqlParser**: Parsing de arquivos SQL com extracao de refs, sources e configs
- **JinjaParser**: Analise de templates Jinja em SQL dbt
- **MacroExpander**: Parsing e catalogo de macros Jinja
- **SourceParser**: Parser especializado para sources dbt
- **TestExtractor**: Extracao de testes schema e data tests
- **SchemaExtractor**: Extracao estruturada de informacoes de schema.yml

### Analyzers

- **GraphResolver**: Construcao e consulta de grafos de dependencia com networkx
- **LineageTracker**: Rastreamento de linhagem de dados
- **DependencyAnalyzer**: Analise avancada de dependencias entre modelos
- **RefResolver**: Resolucao de referencias ref() e source()

### Validators

- **ModelValidator**: Validacao de modelos contra boas praticas

## Fluxo de Dados

```
Arquivos dbt -> Parsers -> Estruturas de Dados -> Analyzers -> Resultados
                                                -> Validators -> Relatorios
```

## Dependencias Externas

- **pyyaml**: Parsing de YAML
- **networkx**: Grafos de dependencia

## Estrutura de Diretorios

```
dbt_parser/
  parsers/       - Modulos de parsing (YAML, SQL, Jinja)
  analyzers/     - Modulos de analise (grafos, linhagem, dependencias)
  validators/    - Modulos de validacao (modelos, nomes, configs)
  exporters/     - Modulos de exportacao (JSON, GraphViz, Mermaid)
  utils/         - Utilitarios (cache, busca)
tests/           - Suite de testes com pytest
docs/            - Documentacao do projeto
```
