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
    FROM {{ source('oscar_raw_json', 'jsn_mstore_history_t_troca') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:op::VARCHAR AS op,
    JSON_DATA:timestamp::VARCHAR AS timestamp,
    JSON_DATA:idtroca::NUMBER AS idtroca,
    JSON_DATA:status::VARCHAR AS status,
    JSON_DATA:data::TIMESTAMP_NTZ AS data,
    JSON_DATA:idorcamento::NUMBER AS idorcamento,
    JSON_DATA:nomecliente::VARCHAR AS nomecliente,
    JSON_DATA:idestabelecimento::NUMBER AS idestabelecimento,
    JSON_DATA:idpontovenda::NUMBER AS idpontovenda,
    JSON_DATA:idusuario::NUMBER AS idusuario,
    JSON_DATA:quantidadeprodutos::NUMBER AS quantidadeprodutos,
    JSON_DATA:valortotalprodutos::NUMBER AS valortotalprodutos,
    JSON_DATA:istrocado::VARCHAR AS istrocado,
    JSON_DATA:idusuariocancelamento::NUMBER AS idusuariocancelamento,
    JSON_DATA:datacancelamento::TIMESTAMP_NTZ AS datacancelamento,
    JSON_DATA:idcaixa::NUMBER AS idcaixa,
    JSON_DATA:codigocliente::NUMBER AS codigocliente,
    JSON_DATA:idcliente::NUMBER AS idcliente,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:motivocancelamento::VARCHAR AS motivocancelamento,
    JSON_DATA:idnfe::NUMBER AS idnfe,
    JSON_DATA:codigoexterno::VARCHAR AS codigoexterno,
    JSON_DATA:idnfedevolucao::NUMBER AS idnfedevolucao,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data