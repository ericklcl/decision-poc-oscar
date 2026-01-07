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
    FROM {{ source('raw_json', 'jsn_mstore_t_troca') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:IDTROCA::NUMBER AS IDTROCA,
    JSON_DATA:STATUS::VARCHAR AS STATUS,
    JSON_DATA:DATA::TIMESTAMP_NTZ AS DATA,
    JSON_DATA:IDORCAMENTO::NUMBER AS IDORCAMENTO,
    JSON_DATA:NOMECLIENTE::VARCHAR AS NOMECLIENTE,
    JSON_DATA:IDESTABELECIMENTO::NUMBER AS IDESTABELECIMENTO,
    JSON_DATA:IDPONTOVENDA::NUMBER AS IDPONTOVENDA,
    JSON_DATA:IDUSUARIO::NUMBER AS IDUSUARIO,
    JSON_DATA:QUANTIDADEPRODUTOS::NUMBER AS QUANTIDADEPRODUTOS,
    JSON_DATA:VALORTOTALPRODUTOS::NUMBER AS VALORTOTALPRODUTOS,
    JSON_DATA:ISTROCADO::VARCHAR AS ISTROCADO,
    JSON_DATA:IDUSUARIOCANCELAMENTO::NUMBER AS IDUSUARIOCANCELAMENTO,
    JSON_DATA:DATACANCELAMENTO::TIMESTAMP_NTZ AS DATACANCELAMENTO,
    JSON_DATA:IDCAIXA::NUMBER AS IDCAIXA,
    JSON_DATA:CODIGOCLIENTE::NUMBER AS CODIGOCLIENTE,
    JSON_DATA:IDCLIENTE::NUMBER AS IDCLIENTE,
    JSON_DATA:CODIGOAB::NUMBER AS CODIGOAB,
    JSON_DATA:MOTIVOCANCELAMENTO::VARCHAR AS MOTIVOCANCELAMENTO,
    JSON_DATA:IDNFE::NUMBER AS IDNFE,
    JSON_DATA:CODIGOEXTERNO::VARCHAR AS CODIGOEXTERNO,
    JSON_DATA:IDNFEDEVOLUCAO::NUMBER AS IDNFEDEVOLUCAO,
    JSON_DATA:_DMS_LOADED_AT::TIMESTAMP_NTZ AS _DMS_LOADED_AT,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data