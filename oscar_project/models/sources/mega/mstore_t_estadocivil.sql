{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_estadocivil AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_estadocivil') }}
)

SELECT *
FROM mstore_t_estadocivil
