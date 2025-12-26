{{
  config(
    materialized = 'table'
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
    FROM OSCAR_RAW_JSON.JSN_MSTORE_T_CONTATOCLIENTE
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:idcontatocliente::NUMBER AS idcontatocliente,
    JSON_DATA:idcliente::NUMBER AS idcliente,
    JSON_DATA:idtipocontato::NUMBER AS idtipocontato,
    JSON_DATA:status::VARCHAR AS status,
    JSON_DATA:descricao::VARCHAR AS descricao,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:observacao::VARCHAR AS observacao,
    JSON_DATA:ddd::VARCHAR AS ddd,
    JSON_DATA:enviasms::VARCHAR AS enviasms,
    JSON_DATA:enviaemail::VARCHAR AS enviaemail,
    JSON_DATA:validoemail::VARCHAR AS validoemail,
    JSON_DATA:codigoexterno::VARCHAR AS codigoexterno,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data