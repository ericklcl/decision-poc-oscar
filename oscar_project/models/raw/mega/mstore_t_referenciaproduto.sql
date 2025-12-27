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
    FROM {{ source('raw_json', 'jsn_mstore_t_referenciaproduto') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:idreferenciaproduto::NUMBER AS idreferenciaproduto,
    JSON_DATA:referencia::VARCHAR AS referencia,
    JSON_DATA:idgrupoproduto::NUMBER AS idgrupoproduto,
    JSON_DATA:idprodutogenerico::NUMBER AS idprodutogenerico,
    JSON_DATA:idprodutoespecifico::NUMBER AS idprodutoespecifico,
    JSON_DATA:idmarca::NUMBER AS idmarca,
    JSON_DATA:idmaterial::NUMBER AS idmaterial,
    JSON_DATA:idunidade::NUMBER AS idunidade,
    JSON_DATA:idcomprador::NUMBER AS idcomprador,
    JSON_DATA:tiponacionalidade::VARCHAR AS tiponacionalidade,
    JSON_DATA:tipocodigobarra::NUMBER AS tipocodigobarra,
    JSON_DATA:idsecao::NUMBER AS idsecao,
    JSON_DATA:descricaoresumida::VARCHAR AS descricaoresumida,
    JSON_DATA:status::VARCHAR AS status,
    JSON_DATA:idgrade::NUMBER AS idgrade,
    JSON_DATA:idtributacao::NUMBER AS idtributacao,
    JSON_DATA:idfornecedor::NUMBER AS idfornecedor,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:substituicaotributaria::VARCHAR AS substituicaotributaria,
    JSON_DATA:ncm::VARCHAR AS ncm,
    JSON_DATA:origemproduto::VARCHAR AS origemproduto,
    JSON_DATA:reducaobaseicms::NUMBER AS reducaobaseicms,
    JSON_DATA:tipo::VARCHAR AS tipo,
    JSON_DATA:cest::VARCHAR AS cest,
    JSON_DATA:urlecommerce::VARCHAR AS urlecommerce,
    JSON_DATA:datacadastro::TIMESTAMP_NTZ AS datacadastro,
    JSON_DATA:dataprimeiraentrada::TIMESTAMP_NTZ AS dataprimeiraentrada,
    JSON_DATA:statusprodutonovo::VARCHAR AS statusprodutonovo,
    JSON_DATA:dataprodutoantigo::TIMESTAMP_NTZ AS dataprodutoantigo,
    JSON_DATA:codigoexterno::VARCHAR AS codigoexterno,
    JSON_DATA:produtodiadora::NUMBER AS produtodiadora,
    JSON_DATA:codigoexternopaqueta::VARCHAR AS codigoexternopaqueta,
    JSON_DATA:cstpiscofinsvenda::VARCHAR AS cstpiscofinsvenda,
    JSON_DATA:cstpiscofinsremessa::VARCHAR AS cstpiscofinsremessa,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data