{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_metaprodutosportal AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_metaprodutosportal') }}
)

SELECT *
FROM mstore_t_metaprodutosportal
