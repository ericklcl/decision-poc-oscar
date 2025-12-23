{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_endereco AS (

    SELECT
      *

    FROM {{ source('raw_input', 'mstore_t_endereco') }}

)

SELECT *
FROM mstore_t_endereco