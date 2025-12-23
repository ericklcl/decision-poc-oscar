{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_cor AS (

    SELECT
      *

    FROM {{ source('raw_input', 'mstore_t_cor') }}

)

SELECT *
FROM mstore_t_cor