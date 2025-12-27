{{
  config(
    materialized = 'table'
  )
}}

WITH mstore_t_orcamentotroca AS (
    
    SELECT
      codigoab,
      idtroca,
      idorcamento,
      idorcamentotroca

    FROM {{ ref('mstore_t_orcamentotroca') }}
)

SELECT * FROM mstore_t_orcamentotroca