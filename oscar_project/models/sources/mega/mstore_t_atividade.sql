{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_atividade AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_atividade') }}
)

SELECT *
FROM mstore_t_atividade
