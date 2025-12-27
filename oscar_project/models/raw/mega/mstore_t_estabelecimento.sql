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
    JSON_DATA:idestabelecimento::NUMBER AS idestabelecimento,
    JSON_DATA:status::VARCHAR AS status,
    JSON_DATA:razaosocial::VARCHAR AS razaosocial,
    JSON_DATA:nomefantasia::VARCHAR AS nomefantasia,
    JSON_DATA:cnpj::VARCHAR AS cnpj,
    JSON_DATA:inscricaoestadual::VARCHAR AS inscricaoestadual,
    JSON_DATA:idendereco::NUMBER AS idendereco,
    JSON_DATA:sigla::VARCHAR AS sigla,
    JSON_DATA:observacao::VARCHAR AS observacao,
    JSON_DATA:tipo::VARCHAR AS tipo,
    JSON_DATA:idunidadenegocio::NUMBER AS idunidadenegocio,
    JSON_DATA:defeito::VARCHAR AS defeito,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:crt::NUMBER AS crt,
    JSON_DATA:idcsc::VARCHAR AS idcsc,
    JSON_DATA:csc::VARCHAR AS csc,
    JSON_DATA:logomarca::VARCHAR AS logomarca,
    JSON_DATA:codigolojacards::VARCHAR AS codigolojacards,
    JSON_DATA:permitirmovimentacao::VARCHAR AS permitirmovimentacao,
    JSON_DATA:nomemobile::VARCHAR AS nomemobile,
    JSON_DATA:longitude::VARCHAR AS longitude,
    JSON_DATA:latitude::VARCHAR AS latitude,
    JSON_DATA:tptrib::NUMBER AS tptrib,
    JSON_DATA:idgrupof::NUMBER AS idgrupof,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data