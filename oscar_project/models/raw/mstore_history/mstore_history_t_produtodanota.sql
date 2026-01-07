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
    FROM {{ source('raw_json', 'jsn_mstore_history_t_produtodanota') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:IDPRODUTODANOTA::NUMBER AS IDPRODUTODANOTA,
    JSON_DATA:IPI::NUMBER AS IPI,
    JSON_DATA:PRECODEVENDA::NUMBER AS PRECODEVENDA,
    JSON_DATA:PRECODECUSTO::NUMBER AS PRECODECUSTO,
    JSON_DATA:QUANTIDADE::NUMBER AS QUANTIDADE,
    JSON_DATA:IDPRODUTO::NUMBER AS IDPRODUTO,
    JSON_DATA:IDNOTAFISCAL::NUMBER AS IDNOTAFISCAL,
    JSON_DATA:PRECOCUSTOPEDIDO::NUMBER AS PRECOCUSTOPEDIDO,
    JSON_DATA:QUANTIDADEITENSEXTRA::NUMBER AS QUANTIDADEITENSEXTRA,
    JSON_DATA:QUANTIDADEITENSPERDIDA::NUMBER AS QUANTIDADEITENSPERDIDA,
    JSON_DATA:IDPEDIDO::NUMBER AS IDPEDIDO,
    JSON_DATA:ACRESCIMO::NUMBER AS ACRESCIMO,
    JSON_DATA:DESCONTO::NUMBER AS DESCONTO,
    JSON_DATA:CODIGOBARRA::VARCHAR AS CODIGOBARRA,
    JSON_DATA:NOTAFISCALCANCELADA::VARCHAR AS NOTAFISCALCANCELADA,
    JSON_DATA:FORADELINHA::VARCHAR AS FORADELINHA,
    JSON_DATA:PRECODECUSTOREAL::NUMBER AS PRECODECUSTOREAL,
    JSON_DATA:CLOSEOUT::VARCHAR AS CLOSEOUT,
    JSON_DATA:CODIGOAB::NUMBER AS CODIGOAB,
    JSON_DATA:CPROD::VARCHAR AS CPROD,
    JSON_DATA:NCM::VARCHAR AS NCM,
    JSON_DATA:ORIGEM::VARCHAR AS ORIGEM,
    JSON_DATA:OP::VARCHAR AS OP,
    JSON_DATA:_DMS_LOADED_AT::TIMESTAMP_NTZ AS _DMS_LOADED_AT,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data