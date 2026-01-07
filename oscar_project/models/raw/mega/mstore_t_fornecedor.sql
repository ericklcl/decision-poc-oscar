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
    FROM {{ source('raw_json', 'jsn_mstore_t_fornecedor') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:IDFORNECEDOR::NUMBER AS IDFORNECEDOR,
    JSON_DATA:IDTIPOPESSOA::NUMBER AS IDTIPOPESSOA,
    JSON_DATA:IDATIVIDADE::NUMBER AS IDATIVIDADE,
    JSON_DATA:IDCONJUGE::NUMBER AS IDCONJUGE,
    JSON_DATA:IDESTADOCIVIL::NUMBER AS IDESTADOCIVIL,
    JSON_DATA:IDENDERECO::NUMBER AS IDENDERECO,
    JSON_DATA:STATUS::VARCHAR AS STATUS,
    JSON_DATA:CPFCNPJ::VARCHAR AS CPFCNPJ,
    JSON_DATA:RG::VARCHAR AS RG,
    JSON_DATA:NOME::VARCHAR AS NOME,
    JSON_DATA:NOMEREDUZIDO::VARCHAR AS NOMEREDUZIDO,
    JSON_DATA:SEXO::VARCHAR AS SEXO,
    JSON_DATA:MAE::VARCHAR AS MAE,
    JSON_DATA:PAI::VARCHAR AS PAI,
    JSON_DATA:DATANASCIMENTO::TIMESTAMP_NTZ AS DATANASCIMENTO,
    JSON_DATA:INSCRICAOESTADUAL::VARCHAR AS INSCRICAOESTADUAL,
    JSON_DATA:OBSERVACAO::VARCHAR AS OBSERVACAO,
    JSON_DATA:IDFORNECEDORECOMMERCE::NUMBER AS IDFORNECEDORECOMMERCE,
    JSON_DATA:CODIGOAB::NUMBER AS CODIGOAB,
    JSON_DATA:CODIGOEXTERNO::VARCHAR AS CODIGOEXTERNO,
    JSON_DATA:_DMS_LOADED_AT::TIMESTAMP_NTZ AS _DMS_LOADED_AT,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data