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
    FROM {{ source('oscar_raw_json', 'jsn_mstore_t_produtotroca') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:idprodutotroca::NUMBER AS idprodutotroca,
    JSON_DATA:status::VARCHAR AS status,
    JSON_DATA:idproduto::NUMBER AS idproduto,
    JSON_DATA:quantidade::NUMBER AS quantidade,
    JSON_DATA:idtroca::NUMBER AS idtroca,
    JSON_DATA:valor::NUMBER AS valor,
    JSON_DATA:idmotivotroca::NUMBER AS idmotivotroca,
    JSON_DATA:idusuariodevolucao::NUMBER AS idusuariodevolucao,
    JSON_DATA:datadevolucao::TIMESTAMP_NTZ AS datadevolucao,
    JSON_DATA:observacao::VARCHAR AS observacao,
    JSON_DATA:foradelinha::VARCHAR AS foradelinha,
    JSON_DATA:closeout::VARCHAR AS closeout,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:codigoexterno::VARCHAR AS codigoexterno,
    JSON_DATA:customedioproduto::NUMBER AS customedioproduto,
    JSON_DATA:customedioprodutoconsolidado::NUMBER AS customedioprodutoconsolidado,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data