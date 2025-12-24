{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_orcamento AS (

    SELECT
      idorcamento,
      status,
      data,
      tempo,
      observacao,
      idvendedor,
      descontopercentual,
      descontovalor,
      totaldepecas,
      valorbruto,
      valor,
      idusuario,
      idusuarioautorizacao,
      idusuariocancelamento,
      datacancelamento,
      idestabelecimento,
      idpontovenda,
      valefuncionario,
      nomecliente,
      idcondicaopagamento,
      perfilcliente,
      idtroca,
      observacaocancelamento,
      acrescimopercentual,
      codigocondicaopagamentocards,
      descricondicaopagamentocards,
      retiradodoestoque,
      idcaixa,
      idvenda,
      idcliente,
      idusuariocaixa,
      dataconfirmacao,
      codigoab,
      cpfcnpj,
      idultimohistoricospcscore,
      idultimohistoricopesquisaspc,
      ticket,
      statusconferencia,
      idusuarioconferencia,
      valorcupomdesconto,
      valorfrete,
      chaveseguranca,
      codigovalefuncionario,
      cpfcnpjdependente,
      valorkitdesconto,
      idcupomdescontocliente,
      valorcashback,
      autoatendimento,
      tipocupomdesconto,
      idtransacaosolucx,
      orcamentoeditadoflag,
      orcamentowebstoreflag,
      integrador,
      orcamentobaixado,
      idlojavendedor,
      codigoexterno,
      descontofidelidade,
      codigoresgatefidelidade,
      statusresgatefidelidade,
      pedidoecommerce,
      _dms_loaded_at::TIMESTAMP

    FROM {{ source('raw_input', 'mstore_t_orcamento') }}

), orcamento_history AS (

    SELECT
      idorcamento,
      status,
      data,
      tempo,
      observacao,
      idvendedor,
      descontopercentual,
      descontovalor,
      totaldepecas,
      valorbruto,
      valor,
      idusuario,
      idusuarioautorizacao,
      idusuariocancelamento,
      datacancelamento,
      idestabelecimento,
      idpontovenda,
      valefuncionario,
      nomecliente,
      idcondicaopagamento,
      perfilcliente,
      idtroca,
      observacaocancelamento,
      acrescimopercentual,
      codigocondicaopagamentocards,
      descricondicaopagamentocards,
      retiradodoestoque,
      idcaixa,
      idvenda,
      idcliente,
      idusuariocaixa,
      dataconfirmacao,
      codigoab,
      cpfcnpj,
      idultimohistoricospcscore,
      idultimohistoricopesquisaspc,
      ticket,
      statusconferencia,
      idusuarioconferencia,
      valorcupomdesconto,
      valorfrete,
      chaveseguranca,
      codigovalefuncionario,
      cpfcnpjdependente,
      valorkitdesconto,
      idcupomdescontocliente,
      valorcashback,
      autoatendimento,
      tipocupomdesconto,
      idtransacaosolucx,
      orcamentoeditadoflag,
      orcamentowebstoreflag,
      integrador,
      orcamentobaixado,
      idlojavendedor,
      codigoexterno,
      descontofidelidade,
      codigoresgatefidelidade,
      statusresgatefidelidade,
      pedidoecommerce,
      _dms_loaded_at::TIMESTAMP

    FROM {{ source('raw_input', 'mstore_history_t_orcamento') }}
    WHERE data >= '2021-01-01'
), union_orcamento AS (

    SELECT *
    FROM mstore_t_orcamento
    UNION ALL 
    SELECT *
    FROM orcamento_history

)

SELECT *
FROM union_orcamento