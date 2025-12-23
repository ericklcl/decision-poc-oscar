{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_notafiscal AS (

    SELECT
      idnotafiscal,
      numeronota,
      icms,
      valorbaseicms,
      valoricms,
      valorbaseicmssubstituicao,
      valoricmssubstituicao,
      valortotalprodutos,
      valorfrete,
      valorseguro,
      outrasdespesas,
      valortotalipi,
      valortotalnota,
      observacao,
      "data",
      idestabelecimento,
      dataemissao,
      dataconfirmacao,
      idusuarioconfirmacao,
      idconhecimento,
      jaalteroucodigobarra,
      idusuariocadastro,
      divergencias,
      idusuariocancelamento,
      datacancelamento,
      status,
      porcentagemdesconto,
      descricaodivergencias,
      notacarteira,
      tipoipi,
      idfornecedor,
      cnpj,
      codigoab,
      numeronfe,
      porcentagemdereducaodebase,
      usadonfedevolucao,
      isbonificado,
      serie,
      _dms_loaded_at

    FROM {{ source('raw_input', 'mstore_t_notafiscal') }}

), notafiscal_history AS (

    SELECT
      idnotafiscal,
      numeronota,
      icms,
      valorbaseicms,
      valoricms,
      valorbaseicmssubstituicao,
      valoricmssubstituicao,
      valortotalprodutos,
      valorfrete,
      valorseguro,
      outrasdespesas,
      valortotalipi,
      valortotalnota,
      observacao,
      "data",
      idestabelecimento,
      dataemissao,
      dataconfirmacao,
      idusuarioconfirmacao,
      idconhecimento,
      jaalteroucodigobarra,
      idusuariocadastro,
      divergencias,
      idusuariocancelamento,
      datacancelamento,
      status,
      porcentagemdesconto,
      descricaodivergencias,
      notacarteira,
      tipoipi,
      idfornecedor,
      cnpj,
      codigoab,
      numeronfe,
      porcentagemdereducaodebase,
      usadonfedevolucao,
      isbonificado,
      serie,
      _dms_loaded_at

    FROM {{ source('raw_input', 'mstore_history_t_notafiscal') }}
    WHERE data >= '2021-01-01'

), union_nota AS (

    SELECT *
    FROM mstore_t_notafiscal
    UNION ALL 
    SELECT *
    FROM notafiscal_history

)

SELECT *
FROM union_nota