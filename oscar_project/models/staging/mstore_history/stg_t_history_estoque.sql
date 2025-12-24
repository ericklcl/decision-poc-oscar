{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_history_estoque AS (

    SELECT
     *

    FROM {{ ref('mstore_history_estoque') }}
)

SELECT *
FROM mstore_t_history_estoque