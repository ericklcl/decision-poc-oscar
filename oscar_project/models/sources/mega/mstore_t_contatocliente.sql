{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_contatocliente AS (

    SELECT
      *

    FROM {{ source('raw_input', 'mstore_t_contatocliente') }}

)

SELECT *
FROM mstore_t_contatocliente