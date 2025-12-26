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
    FROM OSCAR_RAW_JSON.JSN_MSTORE_T_FUNCIONARIO
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:idfuncionario::NUMBER AS idfuncionario,
    JSON_DATA:comissaoacimameta::NUMBER AS comissaoacimameta,
    JSON_DATA:comissaoentremetas::NUMBER AS comissaoentremetas,
    JSON_DATA:comissaoabaixometa::NUMBER AS comissaoabaixometa,
    JSON_DATA:limitecompras::NUMBER AS limitecompras,
    JSON_DATA:comprasrealizadas::NUMBER AS comprasrealizadas,
    JSON_DATA:porcentagemmaximadesconto::NUMBER AS porcentagemmaximadesconto,
    JSON_DATA:idusuario::NUMBER AS idusuario,
    JSON_DATA:idestabelecimento::NUMBER AS idestabelecimento,
    JSON_DATA:salario::NUMBER AS salario,
    JSON_DATA:datanascimento::TIMESTAMP_NTZ AS datanascimento,
    JSON_DATA:cpf::VARCHAR AS cpf,
    JSON_DATA:conjuge::VARCHAR AS conjuge,
    JSON_DATA:mae::VARCHAR AS mae,
    JSON_DATA:pai::VARCHAR AS pai,
    JSON_DATA:sexo::VARCHAR AS sexo,
    JSON_DATA:rg::VARCHAR AS rg,
    JSON_DATA:idatividade::NUMBER AS idatividade,
    JSON_DATA:nome::VARCHAR AS nome,
    JSON_DATA:nomereduzido::VARCHAR AS nomereduzido,
    JSON_DATA:observacao::VARCHAR AS observacao,
    JSON_DATA:idendereco::NUMBER AS idendereco,
    JSON_DATA:status::VARCHAR AS status,
    JSON_DATA:idfuncionarioold::VARCHAR AS idfuncionarioold,
    JSON_DATA:idestadocivil::NUMBER AS idestadocivil,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:centrodecusto::VARCHAR AS centrodecusto,
    JSON_DATA:cadastradocontadigital::VARCHAR AS cadastradocontadigital,
    JSON_DATA:atualizacadastrobeehome::VARCHAR AS atualizacadastrobeehome,
    JSON_DATA:codigoexterno::VARCHAR AS codigoexterno,
    JSON_DATA:idcadastrobeehome::NUMBER AS idcadastrobeehome,
    JSON_DATA:dataadmissao::TIMESTAMP_NTZ AS dataadmissao,
    JSON_DATA:isbackoffice::VARCHAR AS isbackoffice,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data