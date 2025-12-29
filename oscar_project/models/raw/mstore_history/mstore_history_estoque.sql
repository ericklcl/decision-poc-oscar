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
    JSON_DATA:sk_produto::NUMBER AS sk_produto,
    JSON_DATA:sk_data::VARCHAR AS sk_data,
    JSON_DATA:sk_loja::NUMBER AS sk_loja,
    JSON_DATA:qtd_estoque_pecas::NUMBER AS qtd_estoque_pecas,
    JSON_DATA:vlr_estoque_venda::VARCHAR AS vlr_estoque_venda,
    JSON_DATA:vlr_estoque_custo_sem_imposto::VARCHAR AS vlr_estoque_custo_sem_imposto,
    JSON_DATA:vlr_estoque_custo_com_imposto::VARCHAR AS vlr_estoque_custo_com_imposto,
    JSON_DATA:num_idade_estoque::VARCHAR AS num_idade_estoque,
    JSON_DATA:ds_faixa_de_idade_dd::VARCHAR AS ds_faixa_de_idade_dd
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data