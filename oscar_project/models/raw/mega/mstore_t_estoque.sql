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
    FROM {{ source('raw_json', 'jsn_mstore_t_estoque') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:idestoque::NUMBER AS idestoque,
    JSON_DATA:status::VARCHAR AS status,
    JSON_DATA:idestabelecimento::NUMBER AS idestabelecimento,
    JSON_DATA:idproduto::NUMBER AS idproduto,
    JSON_DATA:quantidade::NUMBER AS quantidade,
    JSON_DATA:codigoantigo::NUMBER AS codigoantigo,
    JSON_DATA:precovenda::NUMBER AS precovenda,
    JSON_DATA:precopromocao::NUMBER AS precopromocao,
    JSON_DATA:codigobarra::VARCHAR AS codigobarra,
    JSON_DATA:promocao::VARCHAR AS promocao,
    JSON_DATA:icms::NUMBER AS icms,
    JSON_DATA:primeiroprecovenda::NUMBER AS primeiroprecovenda,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:migradoseek::VARCHAR AS migradoseek,
    JSON_DATA:valorcustomedio::NUMBER AS valorcustomedio,
    JSON_DATA:codigoexterno::VARCHAR AS codigoexterno,
    JSON_DATA:valorultimocustomedio::NUMBER AS valorultimocustomedio,
    JSON_DATA:valorcusto::NUMBER AS valorcusto,
    JSON_DATA:codigoexternopaqueta::VARCHAR AS codigoexternopaqueta,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data