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
    FROM {{ source('raw_json', 'jsn_mstore_t_estabelecimento') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:IDESTABELECIMENTO::NUMBER AS IDESTABELECIMENTO,
    JSON_DATA:STATUS::VARCHAR AS STATUS,
    JSON_DATA:RAZAOSOCIAL::VARCHAR AS RAZAOSOCIAL,
    JSON_DATA:NOMEFANTASIA::VARCHAR AS NOMEFANTASIA,
    JSON_DATA:CNPJ::VARCHAR AS CNPJ,
    JSON_DATA:INSCRICAOESTADUAL::VARCHAR AS INSCRICAOESTADUAL,
    JSON_DATA:IDENDERECO::NUMBER AS IDENDERECO,
    JSON_DATA:SIGLA::VARCHAR AS SIGLA,
    JSON_DATA:OBSERVACAO::VARCHAR AS OBSERVACAO,
    JSON_DATA:TIPO::VARCHAR AS TIPO,
    JSON_DATA:IDUNIDADENEGOCIO::NUMBER AS IDUNIDADENEGOCIO,
    JSON_DATA:DEFEITO::VARCHAR AS DEFEITO,
    JSON_DATA:CODIGOAB::NUMBER AS CODIGOAB,
    JSON_DATA:CRT::NUMBER AS CRT,
    JSON_DATA:IDCSC::VARCHAR AS IDCSC,
    JSON_DATA:CSC::VARCHAR AS CSC,
    JSON_DATA:LOGOMARCA::VARCHAR AS LOGOMARCA,
    JSON_DATA:CODIGOLOJACARDS::VARCHAR AS CODIGOLOJACARDS,
    JSON_DATA:PERMITIRMOVIMENTACAO::VARCHAR AS PERMITIRMOVIMENTACAO,
    JSON_DATA:NOMEMOBILE::VARCHAR AS NOMEMOBILE,
    JSON_DATA:LONGITUDE::VARCHAR AS LONGITUDE,
    JSON_DATA:LATITUDE::VARCHAR AS LATITUDE,
    JSON_DATA:TPTRIB::NUMBER AS TPTRIB,
    JSON_DATA:IDGRUPOF::NUMBER AS IDGRUPOF,
    JSON_DATA:_DMS_LOADED_AT::TIMESTAMP_NTZ AS _DMS_LOADED_AT,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data