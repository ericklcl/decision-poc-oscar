# Scripts de Automação DBT - Oscar Project

Esta pasta contém scripts Python para automatizar diversas tarefas relacionadas ao projeto DBT Oscar. Cada script tem uma função específica no pipeline de processamento de dados.

## 📁 Scripts Disponíveis

### 1. `generate_json_tables.py` - Gerador de Tabelas JSON

**Função**: Gera automaticamente o arquivo `oscar_json_tables.sql` com definições CREATE TABLE para todas as tabelas JSON do projeto.

**Fontes de dados**:
- `sources.yml` (sources: `raw_input` e `raw_data`)
- Diretório `oscar_raw_json/` (arquivos .sql existentes)

**Uso**:
```bash
# Geração básica
python3 generate_json_tables.py

# Com detalhes
python3 generate_json_tables.py --verbose

# Caminhos personalizados
python3 generate_json_tables.py \
  --sources-path sources.yml \
  --oscar-raw-json-path oscar_raw_json \
  --output-path oscar_json_tables.sql
```

**Resultado**: Arquivo SQL com 42 tabelas no padrão `OSCAR_RAW_JSON.JSN_<NOME_DA_TABELA>` contendo estrutura JSON padrão (JSON_DATA, metadados S3, timestamps).

---

### 2. `generate_json_models.py` - Gerador de Modelos DBT

**Função**: Gera automaticamente modelos DBT que extraem colunas JSON das tabelas OSCAR_RAW_JSON baseado nas definições do `sources.yml`.

**Principais recursos**:
- Lê definições de colunas do `sources.yml`
- Mapeia tipos PostgreSQL → Snowflake
- Gera modelos DBT com extração JSON tipada
- Preserva metadados de carga (S3, timestamps)

**Mapeamento de tipos**:
- `character varying` → `VARCHAR`
- `numeric` → `NUMBER`
- `timestamp without time zone` → `TIMESTAMP_NTZ`
- `smallint` → `NUMBER`

**Uso**:
```bash
python3 generate_json_models.py
```

**Resultado**: Arquivos `.sql` na pasta `oscar_raw_json/` com modelos DBT para extração de campos JSON estruturados.

**Exemplo de saída**:
```sql
SELECT 
    JSON_DATA:id::NUMBER AS id,
    JSON_DATA:nome::VARCHAR AS nome,
    S3_FILE_NAME,
    LOAD_TIMESTAMP_UTC,
    RECORD_SOURCE
FROM {{ ref('jsn_minha_tabela') }}
```

---

### 3. `find_sources_without_columns.py` - Analisador de Definições

**Função**: Identifica tabelas no `sources.yml` que não possuem colunas definidas, auxiliando na completude da documentação.

**Análises realizadas**:
- Tabelas sem seção `columns`
- Tabelas com seção `columns` vazia
- Estatísticas de cobertura

**Uso**:
```bash
python3 find_sources_without_columns.py
```

**Exemplo de saída**:
```
=== SOURCES SEM COLUNAS DEFINIDAS ===

📋 Tabelas SEM a seção 'columns':
   1. mstore_t_situacaoproduto

📊 RESUMO:
   Total de tabelas: 40
   Tabelas sem colunas: 1
   Tabelas com colunas: 39
   Percentual sem colunas: 2.5%
```

## 🔧 Dependências

Todos os scripts requerem:
```bash
pip install pyyaml
```

## 📂 Estrutura de Arquivos

```
oscar_project/scripts/
├── README.md                      # Este arquivo
├── sources.yml                    # Definições de sources DBT
├── oscar_json_tables.sql          # Arquivo gerado/referência
├── generate_json_tables.py        # Gerador de tabelas JSON
├── generate_json_models.py        # Gerador de modelos DBT
├── find_sources_without_columns.py # Analisador de definições
└── oscar_raw_json/                # Modelos DBT gerados
    ├── mstore_t_cliente.sql
    ├── mstore_t_produto.sql
    └── ...
```

## 🚀 Workflow Recomendado

1. **Análise inicial**: Execute `find_sources_without_columns.py` para identificar tabelas sem colunas definidas
2. **Geração de tabelas**: Execute `generate_json_tables.py` para criar estruturas de tabelas JSON
3. **Geração de modelos**: Execute `generate_json_models.py` para criar modelos DBT de extração
4. **Revisão**: Revise e ajuste os modelos gerados conforme necessário

## 💡 Notas Importantes

- Os scripts trabalham com caminhos relativos à pasta `scripts/`
- `generate_json_models.py` só processa tabelas com colunas definidas
- `generate_json_tables.py` combina informações de múltiplas fontes para completude
- Todos os scripts incluem tratamento de erros e logging detalhado

## 🔄 Automação

Para uso em pipelines CI/CD:
```bash
cd oscar_project/scripts
python3 generate_json_tables.py --verbose
python3 generate_json_models.py
```