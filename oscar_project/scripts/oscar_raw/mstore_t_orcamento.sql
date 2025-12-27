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
    FROM {{ source('oscar_raw_json', 'jsn_mstore_t_orcamento') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:idorcamento::NUMBER AS idorcamento,
    JSON_DATA:status::VARCHAR AS status,
    JSON_DATA:data::TIMESTAMP_NTZ AS data,
    JSON_DATA:tempo::NUMBER AS tempo,
    JSON_DATA:observacao::VARCHAR AS observacao,
    JSON_DATA:idvendedor::NUMBER AS idvendedor,
    JSON_DATA:descontopercentual::NUMBER AS descontopercentual,
    JSON_DATA:descontovalor::NUMBER AS descontovalor,
    JSON_DATA:totaldepecas::NUMBER AS totaldepecas,
    JSON_DATA:valorbruto::NUMBER AS valorbruto,
    JSON_DATA:valor::NUMBER AS valor,
    JSON_DATA:idusuario::NUMBER AS idusuario,
    JSON_DATA:idusuarioautorizacao::NUMBER AS idusuarioautorizacao,
    JSON_DATA:idusuariocancelamento::NUMBER AS idusuariocancelamento,
    JSON_DATA:datacancelamento::TIMESTAMP_NTZ AS datacancelamento,
    JSON_DATA:idestabelecimento::NUMBER AS idestabelecimento,
    JSON_DATA:idpontovenda::NUMBER AS idpontovenda,
    JSON_DATA:valefuncionario::VARCHAR AS valefuncionario,
    JSON_DATA:nomecliente::VARCHAR AS nomecliente,
    JSON_DATA:idcondicaopagamento::NUMBER AS idcondicaopagamento,
    JSON_DATA:perfilcliente::NUMBER AS perfilcliente,
    JSON_DATA:idtroca::NUMBER AS idtroca,
    JSON_DATA:observacaocancelamento::VARCHAR AS observacaocancelamento,
    JSON_DATA:acrescimopercentual::NUMBER AS acrescimopercentual,
    JSON_DATA:codigocondicaopagamentocards::NUMBER AS codigocondicaopagamentocards,
    JSON_DATA:descricondicaopagamentocards::VARCHAR AS descricondicaopagamentocards,
    JSON_DATA:retiradodoestoque::VARCHAR AS retiradodoestoque,
    JSON_DATA:idcaixa::NUMBER AS idcaixa,
    JSON_DATA:idvenda::VARCHAR AS idvenda,
    JSON_DATA:idcliente::NUMBER AS idcliente,
    JSON_DATA:idusuariocaixa::NUMBER AS idusuariocaixa,
    JSON_DATA:dataconfirmacao::TIMESTAMP_NTZ AS dataconfirmacao,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:cpfcnpj::VARCHAR AS cpfcnpj,
    JSON_DATA:idultimohistoricospcscore::NUMBER AS idultimohistoricospcscore,
    JSON_DATA:idultimohistoricopesquisaspc::NUMBER AS idultimohistoricopesquisaspc,
    JSON_DATA:ticket::VARCHAR AS ticket,
    JSON_DATA:statusconferencia::VARCHAR AS statusconferencia,
    JSON_DATA:idusuarioconferencia::NUMBER AS idusuarioconferencia,
    JSON_DATA:valorcupomdesconto::NUMBER AS valorcupomdesconto,
    JSON_DATA:valorfrete::NUMBER AS valorfrete,
    JSON_DATA:chaveseguranca::VARCHAR AS chaveseguranca,
    JSON_DATA:codigovalefuncionario::NUMBER AS codigovalefuncionario,
    JSON_DATA:cpfcnpjdependente::VARCHAR AS cpfcnpjdependente,
    JSON_DATA:valorkitdesconto::NUMBER AS valorkitdesconto,
    JSON_DATA:idcupomdescontocliente::NUMBER AS idcupomdescontocliente,
    JSON_DATA:valorcashback::NUMBER AS valorcashback,
    JSON_DATA:autoatendimento::VARCHAR AS autoatendimento,
    JSON_DATA:tipocupomdesconto::NUMBER AS tipocupomdesconto,
    JSON_DATA:idtransacaosolucx::VARCHAR AS idtransacaosolucx,
    JSON_DATA:orcamentoeditadoflag::VARCHAR AS orcamentoeditadoflag,
    JSON_DATA:orcamentowebstoreflag::VARCHAR AS orcamentowebstoreflag,
    JSON_DATA:integrador::VARCHAR AS integrador,
    JSON_DATA:orcamentobaixado::VARCHAR AS orcamentobaixado,
    JSON_DATA:idlojavendedor::NUMBER AS idlojavendedor,
    JSON_DATA:codigoexterno::VARCHAR AS codigoexterno,
    JSON_DATA:descontofidelidade::NUMBER AS descontofidelidade,
    JSON_DATA:codigoresgatefidelidade::VARCHAR AS codigoresgatefidelidade,
    JSON_DATA:statusresgatefidelidade::VARCHAR AS statusresgatefidelidade,
    JSON_DATA:pedidoecommerce::VARCHAR AS pedidoecommerce,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data