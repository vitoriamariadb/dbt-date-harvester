# Changelog

## [1.0.0] - 2023-01-31

### Adicionado
- Parser YAML para schema.yml, sources.yml e dbt_project.yml
- Parser SQL com extracao de refs, sources, configs e CTEs
- Parser Jinja para analise de templates
- Extrator de schema com modelos, colunas e testes
- Parser de sources dedicado
- Extrator de testes (schema tests e data tests)
- Expansor de macros Jinja
- Grafo de dependencias com networkx
- Rastreador de linhagem de dados
- Analisador de dependencias com ordem topologica
- Resolver de referencias ref() e source()
- Analisador de impacto de mudancas
- Detector de modelos nao utilizados
- Metricas de complexidade SQL
- Detector de padroes duplicados
- Validador de modelos contra boas praticas
- Validador de convencoes de nomenclatura
- Validador de configuracoes de projeto
- Exportacao para JSON
- Exportacao para GraphViz DOT
- Exportacao para Mermaid
- Sistema de filtragem avancado com seletores
- Busca fuzzy de modelos
- Cache de resultados em memoria
- Rastreamento de performance
- Sistema de plugins extensivel
- Interface CLI completa (parse, graph, validate, lineage)
- Suite de testes com pytest (cobertura acima de 85%)
- CI/CD com GitHub Actions
- Pre-commit hooks configurados
- Documentacao completa (arquitetura, features, tutorial, api reference)
