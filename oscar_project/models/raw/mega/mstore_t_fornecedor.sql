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
    JSON_DATA:idfornecedor::NUMBER AS idfornecedor,
    JSON_DATA:idtipopessoa::NUMBER AS idtipopessoa,
    JSON_DATA:idatividade::NUMBER AS idatividade,
    JSON_DATA:idconjuge::NUMBER AS idconjuge,
    JSON_DATA:idestadocivil::NUMBER AS idestadocivil,
    JSON_DATA:idendereco::NUMBER AS idendereco,
    JSON_DATA:status::VARCHAR AS status,
    JSON_DATA:cpfcnpj::VARCHAR AS cpfcnpj,
    JSON_DATA:rg::VARCHAR AS rg,
    JSON_DATA:nome::VARCHAR AS nome,
    JSON_DATA:nomereduzido::VARCHAR AS nomereduzido,
    JSON_DATA:sexo::VARCHAR AS sexo,
    JSON_DATA:mae::VARCHAR AS mae,
    JSON_DATA:pai::VARCHAR AS pai,
    JSON_DATA:datanascimento::TIMESTAMP_NTZ AS datanascimento,
    JSON_DATA:inscricaoestadual::VARCHAR AS inscricaoestadual,
    JSON_DATA:observacao::VARCHAR AS observacao,
    JSON_DATA:idfornecedorecommerce::NUMBER AS idfornecedorecommerce,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:codigoexterno::VARCHAR AS codigoexterno,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data