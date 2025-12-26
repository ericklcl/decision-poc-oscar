# Modelos DBT para Extração de Colunas JSON

Este diretório contém 42 modelos DBT que extraem colunas das tabelas JSON do schema `OSCAR_RAW_JSON` e as transformam em colunas estruturadas.

## Estrutura dos Modelos

Cada modelo segue o padrão:

```sql
{{
  config(
    materialized = 'table'
  ) 
}}

WITH raw_data AS (
    SELECT
        JSON_DATA,
        S3_FILE_NAME,
        S3_FILE_ROW_NUMBER,
        S3_FILE_LAST_MODIFIED,
        LOAD_TIMESTAMP_UTC,
        RECORD_SOURCE
    FROM OSCAR_RAW_JSON.JSN_<NOME_DA_TABELA>
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe JSON_DATA:campo::TIPO
    JSON_DATA:campo1::VARCHAR AS campo1,
    JSON_DATA:campo2::NUMBER AS campo2,
    
    -- Metadados preservados
    S3_FILE_NAME,
    S3_FILE_ROW_NUMBER,
    S3_FILE_LAST_MODIFIED,
    LOAD_TIMESTAMP_UTC,
    RECORD_SOURCE
FROM raw_data
```

## Mapeamento de Tipos de Dados

- `character varying` → `VARCHAR`
- `numeric` → `NUMBER`
- `timestamp without time zone` → `TIMESTAMP_NTZ`
- `smallint` → `NUMBER`

## Dependências

- As tabelas JSON correspondentes devem existir no schema `OSCAR_RAW_JSON`
- As tabelas JSON devem ter sido criadas usando os scripts em `scripts/oscar_json_tables.sql`

## Scripts de Geração

- `scripts/generate_json_models.py` - Script Python para gerar modelos automaticamente
- `scripts/oscar_json_tables.sql` - Scripts DDL para criar as tabelas JSON de origem