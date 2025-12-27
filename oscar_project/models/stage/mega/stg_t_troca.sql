{{
  config(
    materialized = 'table'
  )
}}

WITH mstore_t_troca AS (
    
    SELECT
      idnfedevolucao,
      idtroca,
      quantidadeprodutos,
      idpontovenda,
      valortotalprodutos,
      idestabelecimento,
      status,
      idorcamento,
      data::DATE 						                                AS Data,			
      idcliente,
      idnfe,
      datacancelamento::DATE                                AS datacancelamento

    FROM {{ ref('mstore_t_troca') }}
)

SELECT * FROM mstore_t_troca