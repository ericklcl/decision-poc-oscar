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
    FROM {{ source('raw_json', 'jsn_mstore_t_endereco') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:IDENDERECO::NUMBER AS IDENDERECO,
    JSON_DATA:CEP::VARCHAR AS CEP,
    JSON_DATA:UF::VARCHAR AS UF,
    JSON_DATA:LOCALIDADE::VARCHAR AS LOCALIDADE,
    JSON_DATA:BAIRRO::VARCHAR AS BAIRRO,
    JSON_DATA:NUMERO::VARCHAR AS NUMERO,
    JSON_DATA:COMPLEMENTO::VARCHAR AS COMPLEMENTO,
    JSON_DATA:LOGRADOURO::VARCHAR AS LOGRADOURO,
    JSON_DATA:CODIGOAB::NUMBER AS CODIGOAB,
    JSON_DATA:TIPOMORADIA::VARCHAR AS TIPOMORADIA,
    JSON_DATA:CODIGOMUNICIPIO::NUMBER AS CODIGOMUNICIPIO,
    JSON_DATA:CODIGOEXTERNO::VARCHAR AS CODIGOEXTERNO,
    JSON_DATA:_DMS_LOADED_AT::TIMESTAMP_NTZ AS _DMS_LOADED_AT,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data