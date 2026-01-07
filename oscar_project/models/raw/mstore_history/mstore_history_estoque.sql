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
    FROM {{ source('raw_json', 'jsn_mstore_history_estoque') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:SK_PRODUTO::NUMBER AS SK_PRODUTO,
    JSON_DATA:SK_DATA::VARCHAR AS SK_DATA,
    JSON_DATA:SK_LOJA::NUMBER AS SK_LOJA,
    JSON_DATA:QTD_ESTOQUE_PECAS::NUMBER AS QTD_ESTOQUE_PECAS,
    JSON_DATA:VLR_ESTOQUE_VENDA::VARCHAR AS VLR_ESTOQUE_VENDA,
    JSON_DATA:VLR_ESTOQUE_CUSTO_SEM_IMPOSTO::VARCHAR AS VLR_ESTOQUE_CUSTO_SEM_IMPOSTO,
    JSON_DATA:VLR_ESTOQUE_CUSTO_COM_IMPOSTO::VARCHAR AS VLR_ESTOQUE_CUSTO_COM_IMPOSTO,
    JSON_DATA:NUM_IDADE_ESTOQUE::VARCHAR AS NUM_IDADE_ESTOQUE,
    JSON_DATA:DS_FAIXA_DE_IDADE_DD::VARCHAR AS DS_FAIXA_DE_IDADE_DD
    JSON_DATA:_DMS_LOADED_AT::TIMESTAMP_NTZ AS _DMS_LOADED_AT,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data