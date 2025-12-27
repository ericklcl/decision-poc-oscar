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
    FROM {{ source('oscar_raw_json', 'jsn_mstore_t_produto') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:idproduto::NUMBER AS idproduto,
    JSON_DATA:dataultimavenda::TIMESTAMP_NTZ AS dataultimavenda,
    JSON_DATA:dataultimaentrada::TIMESTAMP_NTZ AS dataultimaentrada,
    JSON_DATA:foradelinha::VARCHAR AS foradelinha,
    JSON_DATA:nome::VARCHAR AS nome,
    JSON_DATA:nomeimpressao::VARCHAR AS nomeimpressao,
    JSON_DATA:idreferenciaproduto::NUMBER AS idreferenciaproduto,
    JSON_DATA:idcor::NUMBER AS idcor,
    JSON_DATA:idtamanho::NUMBER AS idtamanho,
    JSON_DATA:valorcusto::NUMBER AS valorcusto,
    JSON_DATA:valorcomfrete::NUMBER AS valorcomfrete,
    JSON_DATA:precobase::NUMBER AS precobase,
    JSON_DATA:dataforadelinha::TIMESTAMP_NTZ AS dataforadelinha,
    JSON_DATA:status::VARCHAR AS status,
    JSON_DATA:idproduto_old::NUMBER AS idproduto_old,
    JSON_DATA:codigobarramigracao::VARCHAR AS codigobarramigracao,
    JSON_DATA:dataultimaalteracao::TIMESTAMP_NTZ AS dataultimaalteracao,
    JSON_DATA:idcondpagultimaentrada::NUMBER AS idcondpagultimaentrada,
    JSON_DATA:premio::NUMBER AS premio,
    JSON_DATA:pontos::NUMBER AS pontos,
    JSON_DATA:closeout::VARCHAR AS closeout,
    JSON_DATA:dt_envio_ecommerce::TIMESTAMP_NTZ AS dt_envio_ecommerce,
    JSON_DATA:ecommerce::VARCHAR AS ecommerce,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:iddepartamento::NUMBER AS iddepartamento,
    JSON_DATA:dataultimamovimentacao::TIMESTAMP_NTZ AS dataultimamovimentacao,
    JSON_DATA:ean::VARCHAR AS ean,
    JSON_DATA:codigoalfa::VARCHAR AS codigoalfa,
    JSON_DATA:precovenda::NUMBER AS precovenda,
    JSON_DATA:primeiroprecovenda::NUMBER AS primeiroprecovenda,
    JSON_DATA:migradoizio::NUMBER AS migradoizio,
    JSON_DATA:visivel_ecommerce::VARCHAR AS visivel_ecommerce,
    JSON_DATA:valorcustomedio::NUMBER AS valorcustomedio,
    JSON_DATA:codigoexterno::VARCHAR AS codigoexterno,
    JSON_DATA:codigoexternopaqueta::VARCHAR AS codigoexternopaqueta,
    JSON_DATA:sku_paqueta::VARCHAR AS sku_paqueta,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data