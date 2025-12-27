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
    FROM {{ source('oscar_raw_json', 'jsn_mstore_t_orcamentoproduto') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:idorcamentoproduto::NUMBER AS idorcamentoproduto,
    JSON_DATA:valorvenda::NUMBER AS valorvenda,
    JSON_DATA:idorcamento::NUMBER AS idorcamento,
    JSON_DATA:quantidade::NUMBER AS quantidade,
    JSON_DATA:idproduto::NUMBER AS idproduto,
    JSON_DATA:valorcusto::NUMBER AS valorcusto,
    JSON_DATA:descontovalor::NUMBER AS descontovalor,
    JSON_DATA:foradelinha::VARCHAR AS foradelinha,
    JSON_DATA:premio::NUMBER AS premio,
    JSON_DATA:closeout::VARCHAR AS closeout,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:retiradodoestoquepelatroca::VARCHAR AS retiradodoestoquepelatroca,
    JSON_DATA:conferido::VARCHAR AS conferido,
    JSON_DATA:ehpresente::VARCHAR AS ehpresente,
    JSON_DATA:valorkitdesconto::NUMBER AS valorkitdesconto,
    JSON_DATA:ehkitproduto::VARCHAR AS ehkitproduto,
    JSON_DATA:tipokitprodutoaplicado::NUMBER AS tipokitprodutoaplicado,
    JSON_DATA:agrupamentopresente::VARCHAR AS agrupamentopresente,
    JSON_DATA:ehbrinde::VARCHAR AS ehbrinde,
    JSON_DATA:valorcashback::NUMBER AS valorcashback,
    JSON_DATA:ehkitprodutobrinde::VARCHAR AS ehkitprodutobrinde,
    JSON_DATA:tipokitproduto::NUMBER AS tipokitproduto,
    JSON_DATA:valorcupomdesconto::NUMBER AS valorcupomdesconto,
    JSON_DATA:statusprodutonovo::VARCHAR AS statusprodutonovo,
    JSON_DATA:valordescontofuncionario::NUMBER AS valordescontofuncionario,
    JSON_DATA:freterateado::NUMBER AS freterateado,
    JSON_DATA:codigoexterno::VARCHAR AS codigoexterno,
    JSON_DATA:customedioproduto::NUMBER AS customedioproduto,
    JSON_DATA:customedioprodutoconsolidado::NUMBER AS customedioprodutoconsolidado,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data