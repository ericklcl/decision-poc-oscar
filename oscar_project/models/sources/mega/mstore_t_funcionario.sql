{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_funcionario AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_funcionario') }}
)

SELECT *
FROM mstore_t_funcionario
