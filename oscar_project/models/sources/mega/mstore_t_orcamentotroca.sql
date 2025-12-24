{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_orcamentotroca AS (

    SELECT
      idorcamentotroca,
      idorcamento,
      idtroca,
      codigoab,
      codigoexterno,
      _dms_loaded_at

    FROM {{ source('raw_input', 'mstore_t_orcamentotroca') }}

), orcamentotroca_history AS (

    SELECT
      idorcamentotroca,
      idorcamento,
      idtroca,
      codigoab,
      codigoexterno,
      _dms_loaded_at::TIMESTAMP

    FROM {{ source('raw_input', 'mstore_history_t_orcamentotroca') }}

), union_orcamentotroca AS (

    SELECT *
    FROM mstore_t_orcamentotroca
    UNION ALL 
    SELECT *
    FROM orcamentotroca_history

)

SELECT *
FROM union_orcamentotroca