{{ 
  config(
    materialized = 'table'
  ) 
}}

WITH troca AS (

    SELECT
      idtroca,
      status,
      "data",
      idorcamento,
      nomecliente,
      idestabelecimento,
      idpontovenda,
      idusuario,
      quantidadeprodutos,
      valortotalprodutos,
      istrocado,
      idusuariocancelamento,
      datacancelamento,
      idcaixa,
      codigocliente,
      idcliente,
      codigoab,
      motivocancelamento,
      idnfe,
      codigoexterno,
      idnfedevolucao,
      _dms_loaded_at

    FROM {{ source('raw_input', 'mstore_t_troca') }}

), troca_history AS (

    SELECT
      idtroca,
      status,
      "data",
      idorcamento,
      nomecliente,
      idestabelecimento,
      idpontovenda,
      idusuario,
      quantidadeprodutos,
      valortotalprodutos,
      istrocado,
      idusuariocancelamento,
      datacancelamento,
      idcaixa,
      codigocliente,
      idcliente,
      codigoab,
      motivocancelamento,
      idnfe,
      codigoexterno,
      idnfedevolucao,
      _dms_loaded_at

    FROM {{ source('raw_input', 'mstore_history_t_troca') }}
    WHERE data >= '2021-01-01'

), union_troca AS (

    SELECT *
    FROM troca
    UNION ALL 
    SELECT *
    FROM troca_history

)

SELECT *
FROM union_troca