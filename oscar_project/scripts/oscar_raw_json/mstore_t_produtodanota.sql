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
    FROM OSCAR_RAW_JSON.JSN_MSTORE_T_PRODUTODANOTA
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:idprodutodanota::NUMBER AS idprodutodanota,
    JSON_DATA:ipi::NUMBER AS ipi,
    JSON_DATA:precodevenda::NUMBER AS precodevenda,
    JSON_DATA:precodecusto::NUMBER AS precodecusto,
    JSON_DATA:quantidade::NUMBER AS quantidade,
    JSON_DATA:idproduto::NUMBER AS idproduto,
    JSON_DATA:idnotafiscal::NUMBER AS idnotafiscal,
    JSON_DATA:precocustopedido::NUMBER AS precocustopedido,
    JSON_DATA:quantidadeitensextra::NUMBER AS quantidadeitensextra,
    JSON_DATA:quantidadeitensperdida::NUMBER AS quantidadeitensperdida,
    JSON_DATA:idpedido::NUMBER AS idpedido,
    JSON_DATA:acrescimo::NUMBER AS acrescimo,
    JSON_DATA:desconto::NUMBER AS desconto,
    JSON_DATA:codigobarra::VARCHAR AS codigobarra,
    JSON_DATA:notafiscalcancelada::VARCHAR AS notafiscalcancelada,
    JSON_DATA:foradelinha::VARCHAR AS foradelinha,
    JSON_DATA:precodecustoreal::NUMBER AS precodecustoreal,
    JSON_DATA:closeout::VARCHAR AS closeout,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:cprod::VARCHAR AS cprod,
    JSON_DATA:ncm::VARCHAR AS ncm,
    JSON_DATA:origem::VARCHAR AS origem,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data