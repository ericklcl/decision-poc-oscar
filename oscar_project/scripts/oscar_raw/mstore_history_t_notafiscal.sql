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
    FROM {{ source('oscar_raw_json', 'jsn_mstore_history_t_notafiscal') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:idnotafiscal::NUMBER AS idnotafiscal,
    JSON_DATA:numeronota::VARCHAR AS numeronota,
    JSON_DATA:icms::NUMBER AS icms,
    JSON_DATA:valorbaseicms::NUMBER AS valorbaseicms,
    JSON_DATA:valoricms::NUMBER AS valoricms,
    JSON_DATA:valorbaseicmssubstituicao::NUMBER AS valorbaseicmssubstituicao,
    JSON_DATA:valoricmssubstituicao::NUMBER AS valoricmssubstituicao,
    JSON_DATA:valortotalprodutos::NUMBER AS valortotalprodutos,
    JSON_DATA:valorfrete::NUMBER AS valorfrete,
    JSON_DATA:valorseguro::NUMBER AS valorseguro,
    JSON_DATA:outrasdespesas::NUMBER AS outrasdespesas,
    JSON_DATA:valortotalipi::NUMBER AS valortotalipi,
    JSON_DATA:valortotalnota::NUMBER AS valortotalnota,
    JSON_DATA:observacao::VARCHAR AS observacao,
    JSON_DATA:data::TIMESTAMP_NTZ AS data,
    JSON_DATA:idestabelecimento::NUMBER AS idestabelecimento,
    JSON_DATA:dataemissao::TIMESTAMP_NTZ AS dataemissao,
    JSON_DATA:dataconfirmacao::TIMESTAMP_NTZ AS dataconfirmacao,
    JSON_DATA:idusuarioconfirmacao::NUMBER AS idusuarioconfirmacao,
    JSON_DATA:idconhecimento::NUMBER AS idconhecimento,
    JSON_DATA:jaalteroucodigobarra::VARCHAR AS jaalteroucodigobarra,
    JSON_DATA:idusuariocadastro::NUMBER AS idusuariocadastro,
    JSON_DATA:divergencias::VARCHAR AS divergencias,
    JSON_DATA:idusuariocancelamento::NUMBER AS idusuariocancelamento,
    JSON_DATA:datacancelamento::TIMESTAMP_NTZ AS datacancelamento,
    JSON_DATA:status::VARCHAR AS status,
    JSON_DATA:porcentagemdesconto::NUMBER AS porcentagemdesconto,
    JSON_DATA:descricaodivergencias::VARCHAR AS descricaodivergencias,
    JSON_DATA:notacarteira::VARCHAR AS notacarteira,
    JSON_DATA:tipoipi::VARCHAR AS tipoipi,
    JSON_DATA:idfornecedor::NUMBER AS idfornecedor,
    JSON_DATA:cnpj::VARCHAR AS cnpj,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:numeronfe::VARCHAR AS numeronfe,
    JSON_DATA:porcentagemdereducaodebase::NUMBER AS porcentagemdereducaodebase,
    JSON_DATA:usadonfedevolucao::VARCHAR AS usadonfedevolucao,
    JSON_DATA:isbonificado::VARCHAR AS isbonificado,
    JSON_DATA:serie::VARCHAR AS serie,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    JSON_DATA:op::VARCHAR AS op,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data