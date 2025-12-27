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
    FROM {{ source('oscar_raw_json', 'jsn_mstore_t_cliente') }}
)

SELECT
    -- Colunas extraídas do JSON usando sintaxe Snowflake JSON_DATA:campo::TIPO
    JSON_DATA:idcliente::NUMBER AS idcliente,
    JSON_DATA:idtipopessoa::NUMBER AS idtipopessoa,
    JSON_DATA:idconjuge::NUMBER AS idconjuge,
    JSON_DATA:idempresaclientepf::NUMBER AS idempresaclientepf,
    JSON_DATA:idendereco::NUMBER AS idendereco,
    JSON_DATA:idestabelecimento::NUMBER AS idestabelecimento,
    JSON_DATA:idlimitecredito::NUMBER AS idlimitecredito,
    JSON_DATA:idtamanhocalcado::NUMBER AS idtamanhocalcado,
    JSON_DATA:idtimedefutebol::NUMBER AS idtimedefutebol,
    JSON_DATA:idultimohistoricopesquisaspc::NUMBER AS idultimohistoricopesquisaspc,
    JSON_DATA:idtipocadastro::NUMBER AS idtipocadastro,
    JSON_DATA:idatividade::NUMBER AS idatividade,
    JSON_DATA:idanalisecredito::NUMBER AS idanalisecredito,
    JSON_DATA:idestadocivil::NUMBER AS idestadocivil,
    JSON_DATA:status::VARCHAR AS status,
    JSON_DATA:cpfcnpj::VARCHAR AS cpfcnpj,
    JSON_DATA:rg::VARCHAR AS rg,
    JSON_DATA:rgestado::VARCHAR AS rgestado,
    JSON_DATA:nome::VARCHAR AS nome,
    JSON_DATA:nomereduzido::VARCHAR AS nomereduzido,
    JSON_DATA:sexo::VARCHAR AS sexo,
    JSON_DATA:mae::VARCHAR AS mae,
    JSON_DATA:pai::VARCHAR AS pai,
    JSON_DATA:etnia::VARCHAR AS etnia,
    JSON_DATA:inscricaoestadual::VARCHAR AS inscricaoestadual,
    JSON_DATA:datacadastro::TIMESTAMP_NTZ AS datacadastro,
    JSON_DATA:datanascimento::TIMESTAMP_NTZ AS datanascimento,
    JSON_DATA:dataprimeiracompranolimite::TIMESTAMP_NTZ AS dataprimeiracompranolimite,
    JSON_DATA:dataultimaatualizacao::TIMESTAMP_NTZ AS dataultimaatualizacao,
    JSON_DATA:dataultimacompra::TIMESTAMP_NTZ AS dataultimacompra,
    JSON_DATA:divulgaremail::VARCHAR AS divulgaremail,
    JSON_DATA:isrendacomprovada::VARCHAR AS isrendacomprovada,
    JSON_DATA:pontuacaoantiga::NUMBER AS pontuacaoantiga,
    JSON_DATA:possuiemail::VARCHAR AS possuiemail,
    JSON_DATA:valorrenda::NUMBER AS valorrenda,
    JSON_DATA:qtdcarnespagoematraso::NUMBER AS qtdcarnespagoematraso,
    JSON_DATA:qtdchequesdevolvidos::NUMBER AS qtdchequesdevolvidos,
    JSON_DATA:qtdchequespagoematraso::NUMBER AS qtdchequespagoematraso,
    JSON_DATA:qtdcomprasrealizadas::NUMBER AS qtdcomprasrealizadas,
    JSON_DATA:valorcarnespagoematraso::NUMBER AS valorcarnespagoematraso,
    JSON_DATA:valorchequesdevolvidos::NUMBER AS valorchequesdevolvidos,
    JSON_DATA:valorchequespagoematraso::NUMBER AS valorchequespagoematraso,
    JSON_DATA:valormaximocomprado::NUMBER AS valormaximocomprado,
    JSON_DATA:valortotalcomprado::NUMBER AS valortotalcomprado,
    JSON_DATA:valorultimacompra::NUMBER AS valorultimacompra,
    JSON_DATA:observacao::VARCHAR AS observacao,
    JSON_DATA:codigoab::NUMBER AS codigoab,
    JSON_DATA:desabilitarabandonar::VARCHAR AS desabilitarabandonar,
    JSON_DATA:idultimohistoricospcscore::NUMBER AS idultimohistoricospcscore,
    JSON_DATA:idciclo::NUMBER AS idciclo,
    JSON_DATA:cadastradocards::VARCHAR AS cadastradocards,
    JSON_DATA:contribuinteicms::VARCHAR AS contribuinteicms,
    JSON_DATA:rendacomprovada::VARCHAR AS rendacomprovada,
    JSON_DATA:grupoorigem::VARCHAR AS grupoorigem,
    JSON_DATA:migradofoto::VARCHAR AS migradofoto,
    JSON_DATA:migradoizio::NUMBER AS migradoizio,
    JSON_DATA:idocupacao::NUMBER AS idocupacao,
    JSON_DATA:situacaoempregaticia::NUMBER AS situacaoempregaticia,
    JSON_DATA:utilizaoverlimitfestclub::VARCHAR AS utilizaoverlimitfestclub,
    JSON_DATA:codigoprodutooscar::NUMBER AS codigoprodutooscar,
    JSON_DATA:codigoprodutojo::NUMBER AS codigoprodutojo,
    JSON_DATA:faturadigitaltipo::VARCHAR AS faturadigitaltipo,
    JSON_DATA:orgaoemissor::VARCHAR AS orgaoemissor,
    JSON_DATA:cidadenatal::VARCHAR AS cidadenatal,
    JSON_DATA:ufnatal::VARCHAR AS ufnatal,
    JSON_DATA:dataemissaodocumento::TIMESTAMP_NTZ AS dataemissaodocumento,
    JSON_DATA:integrador::VARCHAR AS integrador,
    JSON_DATA:receberwhatsapp::VARCHAR AS receberwhatsapp,
    JSON_DATA:codigoprodutoabys::NUMBER AS codigoprodutoabys,
    JSON_DATA:possuisenhapadrao::VARCHAR AS possuisenhapadrao,
    JSON_DATA:codigoexterno::VARCHAR AS codigoexterno,
    JSON_DATA:cadastradopointus::VARCHAR AS cadastradopointus,
    JSON_DATA:codigoprodutocarioca::NUMBER AS codigoprodutocarioca,
    JSON_DATA:codigoproduto::NUMBER AS codigoproduto,
    JSON_DATA:isfidelidadepointus::VARCHAR AS isfidelidadepointus,
    JSON_DATA:pinsuframa::NUMBER AS pinsuframa,
    JSON_DATA:fidelidadepointus::VARCHAR AS fidelidadepointus,
    JSON_DATA:_dms_loaded_at::TIMESTAMP_NTZ AS _dms_loaded_at,
    
    -- Metadados preservados para auditoria e rastreabilidade
    S3_FILE_NAME,              -- Arquivo de origem no S3
    S3_FILE_ROW_NUMBER,        -- Posição no arquivo original
    S3_FILE_LAST_MODIFIED,     -- Quando o arquivo foi modificado
    LOAD_TIMESTAMP_UTC,        -- Quando foi carregado no Snowflake
    RECORD_SOURCE              -- Sistema/processo de origem
    
FROM raw_data