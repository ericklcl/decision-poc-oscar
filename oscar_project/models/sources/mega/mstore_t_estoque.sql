{{ 
  config(
    materialized = 'table'
  ) 
}}

WITH estoque AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_estoque') }}

)

SELECT
*
FROM estoque