{{
  config(
    materialized = 'view'
  ) 
}}

WITH raw_data AS (
    SELECT
        JSON_DATA,                  -- Campo VARIANT contendo o JSON completo
        S3_FILE_NAME,              -- Nome do arquivo S3 de origem
        S3_FILE_ROW_NUMBER,        -- Número da linha no arquivo original
        S3_FILE_LAST_MODIFIED,     -- Timestamp de modificação do arquivo S3
        LOAD_TIMESTAMP_UTC,        -- Timestamp de quando foi carregado no Snowflake
        RECORD_SOURCE              -- Identificador da fonte do registro
    FROM {{ source('raw_json', 'jsn_mstore_t_produtotroca') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:IDPRODUTOTROCA::NUMBER AS IDPRODUTOTROCA,
    JSON_DATA:STATUS::VARCHAR AS STATUS,
    JSON_DATA:IDPRODUTO::NUMBER AS IDPRODUTO,
    JSON_DATA:QUANTIDADE::NUMBER AS QUANTIDADE,
    JSON_DATA:IDTROCA::NUMBER AS IDTROCA,
    JSON_DATA:VALOR::NUMBER AS VALOR,
    JSON_DATA:IDMOTIVOTROCA::NUMBER AS IDMOTIVOTROCA,
    JSON_DATA:IDUSUARIODEVOLUCAO::NUMBER AS IDUSUARIODEVOLUCAO,
    JSON_DATA:DATADEVOLUCAO::TIMESTAMP_NTZ AS DATADEVOLUCAO,
    JSON_DATA:OBSERVACAO::VARCHAR AS OBSERVACAO,
    JSON_DATA:FORADELINHA::VARCHAR AS FORADELINHA,
    JSON_DATA:CLOSEOUT::VARCHAR AS CLOSEOUT,
    JSON_DATA:CODIGOAB::NUMBER AS CODIGOAB,
    JSON_DATA:CODIGOEXTERNO::VARCHAR AS CODIGOEXTERNO,
    JSON_DATA:CUSTOMEDIOPRODUTO::NUMBER AS CUSTOMEDIOPRODUTO,
    JSON_DATA:CUSTOMEDIOPRODUTOCONSOLIDADO::NUMBER AS CUSTOMEDIOPRODUTOCONSOLIDADO,
    JSON_DATA:_DMS_LOADED_AT::TIMESTAMP_NTZ AS _DMS_LOADED_AT,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data