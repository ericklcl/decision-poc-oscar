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
    FROM {{ source('raw_json', 'jsn_mstore_t_metaprodutosportal') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:idmetaprodutosportal::NUMBER AS idmetaprodutosportal,
    JSON_DATA:iddepartamento::NUMBER AS iddepartamento,
    JSON_DATA:idgrupoproduto::NUMBER AS idgrupoproduto,
    JSON_DATA:idsecao::NUMBER AS idsecao,
    JSON_DATA:idfamilia::NUMBER AS idfamilia,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data